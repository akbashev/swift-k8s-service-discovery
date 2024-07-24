/*
 Copyright 2021 TupleStream OÜ
 See the LICENSE file for license information
 SPDX-License-Identifier: Apache-2.0
*/
import Foundation
import XCTest
import ServiceDiscovery
import K8sServiceDiscovery
import MicroExpress
import NIOHTTP1

class MockAPIServer {

    static func start() {
        let listResponseFile = Bundle.module.path(forResource: "listresponse", ofType: "json")!
        let url = URL(fileURLWithPath: listResponseFile)
        let body = try! String(contentsOf: url)
        
        let app = Express()
        
        app.get("/api/v1/namespaces/nginx/pods") { req, res, next in
            res.headers = HTTPHeaders([("content-type", "application/json")])
            res.send(body)
        }
        // TODO: Remove task
        Task {
            app.listen(1337)
        }
    }
}

final class K8sServiceDiscoveryTests: XCTestCase {

    let target = K8sObject(labelSelector: ["name":"nginx"], namespace: "nginx")

    func testOneShotLookup() async throws {
        MockAPIServer.start()
        
        try await Task.sleep(for: .seconds(0.5))
        
        let config = K8sDiscoveryConfig(apiUrl: "http://localhost:1337")
        let sd = K8sServiceDiscovery(config: config)

        var output: [K8sPod]? = nil

        sd.lookup(target, deadline: .now() + .milliseconds(2000)) { result in
            switch result {
            case .failure:
                XCTFail("Expected successful response")
            case .success(let instances):
                output = instances
            }
        }

        while output == nil {
            try await Task.sleep(for: .seconds(0.001))
        }

        XCTAssertNotNil(output)
        XCTAssertEqual(1, output!.count)
        XCTAssertTrue(output![0].name.starts(with: "nginx-"))

        // will throw an assertion error if the boxed version doesn't call ds.shutdown() under the hood
        try! ServiceDiscoveryBox<K8sObject, K8sPod>(sd).shutdown()
    }
    
    func testSubscription() async throws {
        guard let _ = ProcessInfo.processInfo.environment["RUN_INTEGRATION_TESTS"] else {
            return
        }

        let k8sManifest = Bundle.module.path(forResource: "integration", ofType: "yml")!
        try shell("kubectl", "apply", "-f", k8sManifest).waitUntilExit()
        try shell("kubectl", "rollout", "status", "deployment/nginx", "-n", "nginx").waitUntilExit()
        let process = try shell("kubectl", "proxy")

        try await Task.sleep(for: .seconds(1))

        var pods = Array<K8sPod>()

        let config = K8sDiscoveryConfig(apiUrl: "http://localhost:8001")
        let sd = K8sServiceDiscovery(config: config)

        let _ = sd.subscribe(to: target) { result in
            // todo
            switch result {
            case .failure:
                XCTFail("Expected successful response")
            case .success(let instances):
                pods.append(contentsOf: instances)
            }
        } onComplete: { reason in
            // no-op
        }

        try await Task.sleep(for: .seconds(0.5))

        XCTAssertEqual(1, pods.count)

        try shell("kubectl", "scale", "--replicas=2", "deployment/nginx", "-n", "nginx").waitUntilExit()
        // wait for rollout again
        try shell("kubectl", "rollout", "status", "deployment/nginx", "-n", "nginx").waitUntilExit()

        try await Task.sleep(for: .seconds(0.5))

        XCTAssertEqual(2, pods.count)

        sd.shutdown()
        process.terminate()
    }

    func testFixedListImpl() {
        let hosts = ["foo.cluster.local"]
        let sd = K8sServiceDiscovery.fromFixedHostList(target: target, hosts: hosts)

        var r: Result<[K8sPod], Error>? = nil

        let callback: (Result<[K8sPod], Error>) -> Void = { res in
            r = res
        }

        sd.lookup(target, callback: callback)

        while r == nil {
            Thread.sleep(forTimeInterval: 0.001)
        }

        let out = try! r!.get()

        XCTAssertEqual(hosts.count, out.count)

        for (idx, pod) in out.enumerated() {
            XCTAssertEqual(pod.address, hosts[idx])
            XCTAssertEqual(pod.name, hosts[idx])
        }

        // shutdown() should be available for a dummy fixed list, but it's a no-op
        try! sd.shutdown()
    }
    
    func shell(_ args: String...) throws -> Process {
        let task = Process()
        task.executableURL = URL(fileURLWithPath: "/usr/bin/env")
        task.arguments = args
        try task.run()
        return task
    }
}
