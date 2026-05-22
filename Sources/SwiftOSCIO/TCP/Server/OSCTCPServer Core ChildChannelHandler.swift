//
//  OSCTCPServer Core ChildChannelHandler.swift
//  SwiftOSC I/O: SwiftNIO • https://github.com/orchetect/swift-osc-io-nio
//  © 2026 Steffan Andrews • Licensed under MIT License
//

import Foundation
import NIO

extension OSCTCPServer.Core {
    final class ChildChannelHandler {
        nonisolated(unsafe)
        weak var server: OSCTCPServer.Core?
        
        /// Internal queue used for synchronizing access to mutable properties.
        let syncQueue = DispatchQueue(label: "com.orchetect.SwiftOSC.OSCTCPServer.Core.ChildChannelHandler.syncQueue", target: .global())
        
        var clientID: OSCTCPClientSessionID {
            get { syncQueue.sync { _clientID } }
            set { syncQueue.sync { _clientID = newValue } }
        }
        nonisolated(unsafe) private var _clientID: OSCTCPClientSessionID = 0

        /// Stores an error captured in `errorCaught` for use in `channelInactive`.
        private var pendingError: (any Error)? {
            get { syncQueue.sync { _pendingError } }
            set { syncQueue.sync { _pendingError = newValue } }
        }
        nonisolated(unsafe) private var _pendingError: (any Error)?

        init(server: OSCTCPServer.Core? = nil) {
            self.server = server
        }
    }
}

extension OSCTCPServer.Core.ChildChannelHandler: Sendable { }

extension OSCTCPServer.Core.ChildChannelHandler: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer

    func channelActive(context: ChannelHandlerContext) {
        guard let server else { return }
        let port = UInt16(context.channel.remoteAddress?.port ?? 0)
        let host = context.channel.remoteAddress?.ipAddress ?? ""

        clientID = server.addClient(channel: context.channel)

        server.generateConnectedNotification(remoteHost: host, remotePort: port, clientID: clientID)
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        var buffer = unwrapInboundIn(data)

        let byteLength = buffer.readableBytes
        guard let bytes = buffer.readBytes(length: byteLength) else { return /* throw error */ }
        let data = Data(bytes)

        guard let server else { return }

        let port = UInt16(context.channel.remoteAddress?.port ?? 0)
        let host = context.channel.remoteAddress?.ipAddress ?? ""

        server.dispatch(receivedTCPFramedData: data, remoteHost: host, remotePort: port)
    }

    func channelInactive(context: ChannelHandlerContext) {
        let error = pendingError
        pendingError = nil

        let port = UInt16(context.channel.remoteAddress?.port ?? 0)
        let host = context.channel.remoteAddress?.ipAddress ?? ""

        server?.disconnectClient(clientID: clientID)
        server?.generateDisconnectedNotification(remoteHost: host, remotePort: port, clientID: clientID, error: error)
    }

    func errorCaught(context: ChannelHandlerContext, error: any Error) {
        pendingError = error
        context.close(promise: nil)
    }
}
