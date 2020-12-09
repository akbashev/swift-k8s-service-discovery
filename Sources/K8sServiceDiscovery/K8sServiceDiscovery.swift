import AsyncHTTPClient
import Dispatch
import Foundation
import NIO
import ServiceDiscovery

fileprivate extension Dictionary where Key == String, Value == String {

    var queryParameters: String {
        var parts: [String] = []
        for (key, value) in self {
            let part = String(format: "%@=%@",
                              String(describing: key).addingPercentEncoding(withAllowedCharacters: .urlQueryAllowed)!,
                              String(describing: value).addingPercentEncoding(withAllowedCharacters: .urlQueryAllowed)!)
            parts.append(part as String)
        }
        return parts.joined(separator: "&")
    }

    var urlEncoded: String {
        if let encoded = queryParameters.addingPercentEncoding(withAllowedCharacters: .alphanumerics) {
            return encoded
        }
        return ""
    }
}

public struct K8sObject: Hashable {
    public var labelSelector: [String:String] = Dictionary()
    public var namespace: String = "default"

    var url: String {
        return "/api/v1/namespaces/\(namespace)/pods?labelSelector=\(labelSelector.urlEncoded)"
    }
}

public struct K8sPod: CustomStringConvertible, Hashable {

    public var name: String
    public var address: String

    public var description: String {
        get {
            return "Pod[\(name) | \(address)]"
        }
    }
}

struct PodMeta: Decodable, Hashable {
    var name: String
}

struct PodStatus: Decodable, Hashable {
    var podIP: String?
}

struct InternalPod: Decodable, Hashable {
    var metadata: PodMeta
    var status: PodStatus

    var publicPod: K8sPod? {
        get {
            if let ip = status.podIP {
                return K8sPod(name: metadata.name, address: ip)
            } else {
                return nil
            }
        }
    }

    static func == (lhs: InternalPod, rhs: InternalPod) -> Bool {
        return lhs.metadata == rhs.metadata
    }

    private enum CodingKeys: String, CodingKey {
        case metadata, status
    }
}

struct PodList: Decodable {
    var items: [InternalPod]

    var publicItems: [K8sPod] {
        var converted = Array<K8sPod>()
        for item in items {
            if let p = item.publicPod {
                converted.append(p)
            }
        }
        return converted
    }

    private enum CodingKeys: String, CodingKey {
        case items
    }
}

enum UpdateOperation: String, Decodable {
    case added = "ADDED"
    case modified = "MODIFIED"
    case deleted = "DELETED"
}

struct PodUpdateOperation: Decodable {
    var type: UpdateOperation
    var object: InternalPod
}

fileprivate extension DispatchTime {

    var asNIODeadline: NIODeadline {
        get {
            .uptimeNanoseconds(self.uptimeNanoseconds)
        }
    }
}

public final class K8sServiceDiscovery: ServiceDiscovery {
    public typealias Service = K8sObject
    public typealias Instance = K8sPod

    public let defaultLookupTimeout: DispatchTimeInterval = .seconds(1)

    private let httpClient = HTTPClient(eventLoopGroupProvider: .createNew)
    private let jsonDecoder = JSONDecoder()
    private let apiHost: String

    public init(apiHost: String) {
        self.apiHost = apiHost
    }

    public func lookup(_ service: K8sObject, deadline: DispatchTime?, callback: @escaping (Result<[K8sPod], Error>) -> Void) {
        httpClient.get(url: fullURL(target: service), deadline: deadline?.asNIODeadline).whenComplete { result in
            let lookupResult: Result<[Instance], Error>!
            switch result {
            case .failure:
                lookupResult = .failure(LookupError.timedOut)
            case .success(let response):
                if let bytes = response.body {
                    let decoded = try! self.jsonDecoder.decode(PodList.self, from: bytes)
                    lookupResult = .success(decoded.publicItems)
                } else {
                    lookupResult = .success([])
                }
            }
            callback(lookupResult)
        }
    }

    private func fullURL(target: K8sObject) -> String {
        return apiHost + target.url
    }

    public func subscribe(to service: K8sObject, onNext nextResultHandler: @escaping (Result<[K8sPod], Error>) -> Void, onComplete completionHandler: @escaping (CompletionReason) -> Void) -> CancellationToken {

        let request = try! HTTPClient.Request(url: self.fullURL(target: service) + "&watch=true")
        let delegate = K8sStreamDelegate(decoder: self.jsonDecoder, onNext: nextResultHandler, onComplete: completionHandler)
        let future = self.httpClient.execute(request: request, delegate: delegate)

        return CancellationToken(isCancelled: false) { _ in
            future.cancel()
        }
    }

    public func shutdown() {
        try! httpClient.syncShutdown()
    }

    class K8sStreamDelegate: HTTPClientResponseDelegate {
        typealias Response = String

        private let decoder: JSONDecoder
        private let nextResultHandler: (Result<[K8sPod], Error>) -> Void
        private let completionHandler: (CompletionReason) -> Void
        private var interimBuffer: ByteBuffer
        private var alreadySeen: Set<K8sPod> = Set()

        init(decoder: JSONDecoder, onNext nextResultHandler: @escaping (Result<[K8sPod], Error>) -> Void, onComplete completionHandler: @escaping (CompletionReason) -> Void) {
            self.decoder = decoder
            self.interimBuffer = ByteBuffer()
            self.nextResultHandler = nextResultHandler
            self.completionHandler = completionHandler
        }

        func didReceiveBodyPart(task: HTTPClient.Task<String>, _ buffer: ByteBuffer) -> EventLoopFuture<Void> {
            // update json objects are newline-delimited, but the contents of a buffer may contain more or less than
            // one exact message; copy to an interim buffer and read up to any occurrences of \n and only decode that,
            // saving any remaining for the next time this is called
            var b = buffer
            interimBuffer.writeBuffer(&b)
            let readable = interimBuffer.withUnsafeReadableBytes { $0.firstIndex(of: UInt8(0x0A)) }
            if let r = readable {
                if let decoded = try! interimBuffer.readJSONDecodable(PodUpdateOperation.self, decoder: decoder, length: r + 1) {
                    switch decoded.type {
                    case .deleted:
                        if let publicPod = decoded.object.publicPod {
                            alreadySeen.remove(publicPod)
                        }
                    case .added:
                        notifyIfNew(decoded.object)
                    case .modified:
                        notifyIfNew(decoded.object)
                    }
                }
            }

            return task.eventLoop.makeSucceededFuture(())
        }

        private func notifyIfNew(_ internalPod: InternalPod) {
            if let publicPod = internalPod.publicPod {
                if !alreadySeen.contains(publicPod) {
                    nextResultHandler(.success([publicPod]))
                    alreadySeen.insert(publicPod)
                }
            }
        }

        func didReceiveError(task: HTTPClient.Task<String>, _ error: Error) {
            completionHandler(.serviceDiscoveryUnavailable)
        }

        func didFinishRequest(task: HTTPClient.Task<String>) throws -> String {
            completionHandler(.serviceDiscoveryUnavailable)
            return ""
        }
    }
}
