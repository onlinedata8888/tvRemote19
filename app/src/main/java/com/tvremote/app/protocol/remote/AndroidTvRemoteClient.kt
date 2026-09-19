package com.tvremote.app.protocol.remote

import com.tvremote.app.protocol.crypto.CertificateManager
import com.tvremote.app.protocol.crypto.TlsSupport
import com.tvremote.app.proto.remote.RemoteConfigure
import com.tvremote.app.proto.remote.RemoteDeviceInfo
import com.tvremote.app.proto.remote.RemoteDirection
import com.tvremote.app.proto.remote.RemoteKeyCode
import com.tvremote.app.proto.remote.RemoteKeyInject
import com.tvremote.app.proto.remote.RemoteMessage
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import javax.net.ssl.SSLSocket

private const val REMOTE_PORT = 6466

/**
 * The live remote-control session. Unlike ADB's "spawn a process per
 * keypress" model, this opens ONE persistent socket straight to the TV's
 * already-running Remote Service — every key press after that is just a
 * few bytes written to an open connection, which is what makes this feel
 * instant instead of the 1-3 second lag ADB-based `input keyevent` has on
 * slower boxes.
 */
class AndroidTvRemoteClient(private val certManager: CertificateManager) {

    private var socket: SSLSocket? = null
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private var readerJob: Job? = null

    val isConnected: Boolean
        get() = socket?.isConnected == true && socket?.isClosed == false

    suspend fun connect(host: String): Result<Unit> = withContext(Dispatchers.IO) {
        try {
            close()
            val sslContext = TlsSupport.buildSslContext(certManager)
            val s = sslContext.socketFactory.createSocket(host, REMOTE_PORT) as SSLSocket
            s.startHandshake()
            socket = s
            startReaderLoop(s)
            Result.success(Unit)
        } catch (t: Throwable) {
            close()
            Result.failure(t)
        }
    }

    /** Keeps the socket alive and answers the TV's handshake/keepalive messages
     *  (configure, set-active, pings) — exactly what the official app does. */
    private fun startReaderLoop(s: SSLSocket) {
        readerJob?.cancel()
        readerJob = scope.launch {
            try {
                while (isConnected) {
                    val msg = RemoteMessage.parseDelimitedFrom(s.inputStream) ?: break
                    when {
                        msg.hasRemoteConfigure() -> {
                            val reply = RemoteMessage.newBuilder().apply {
                                remoteConfigure = RemoteConfigure.newBuilder().apply {
                                    code1 = 622
                                    deviceInfo = RemoteDeviceInfo.newBuilder()
                                        .setModel("TvRemoteApp")
                                        .setVendor("TvRemoteApp")
                                        .setPackageName("com.tvremote.app")
                                        .setAppVersion("1.0")
                                        .build()
                                }.build()
                            }.build()
                            writeMessage(s, reply)
                        }
                        msg.hasRemoteSetActive() -> {
                            val ack = RemoteMessage.newBuilder().apply {
                                remoteSetActive = com.tvremote.app.proto.remote.RemoteSetActive
                                    .newBuilder()
                                    .setActive(622)
                                    .build()
                            }.build()
                            writeMessage(s, ack)
                        }
                        msg.hasRemotePingRequest() -> {
                            val pong = RemoteMessage.newBuilder().apply {
                                remotePingResponse = com.tvremote.app.proto.remote.RemotePingResponse
                                    .newBuilder()
                                    .setVal1(msg.remotePingRequest.val1)
                                    .build()
                            }.build()
                            writeMessage(s, pong)
                        }
                    }
                }
            } catch (_: Throwable) {
                // Socket closed / connection dropped — isConnected will reflect that.
            }
        }
    }

    private fun writeMessage(s: SSLSocket, message: RemoteMessage) {
        synchronized(s) {
            message.writeDelimitedTo(s.outputStream)
            s.outputStream.flush()
        }
    }

    /** Sends one real key press. Fire-and-forget: no round trip to wait for. */
    suspend fun sendKey(keyCode: RemoteKeyCode, direction: RemoteDirection = RemoteDirection.SHORT) {
        withContext(Dispatchers.IO) {
            val s = socket ?: return@withContext
            try {
                val message = RemoteMessage.newBuilder().apply {
                    remoteKeyInject = RemoteKeyInject.newBuilder()
                        .setKeyCode(keyCode)
                        .setDirection(direction)
                        .build()
                }.build()
                writeMessage(s, message)
            } catch (_: Throwable) {
                // Best-effort: a single dropped keypress isn't worth surfacing to the UI.
            }
        }
    }

    fun close() {
        readerJob?.cancel()
        readerJob = null
        try {
            socket?.close()
        } catch (_: Throwable) {
        }
        socket = null
    }
}
