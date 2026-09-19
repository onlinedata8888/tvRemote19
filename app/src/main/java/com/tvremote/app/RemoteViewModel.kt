package com.tvremote.app

import android.app.Application
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import com.tvremote.app.protocol.crypto.CertificateManager
import com.tvremote.app.protocol.discovery.DiscoveredTv
import com.tvremote.app.protocol.discovery.TvDiscovery
import com.tvremote.app.protocol.pairing.AndroidTvPairingClient
import com.tvremote.app.protocol.remote.AndroidTvRemoteClient
import com.tvremote.app.proto.remote.RemoteKeyCode
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch

enum class ConnectionState { DISCONNECTED, SCANNING, CONNECTING, NEEDS_PAIRING_CODE, PAIRING, CONNECTED, ERROR }

data class RemoteUiState(
    val connectionState: ConnectionState = ConnectionState.DISCONNECTED,
    val discoveredTvs: List<DiscoveredTv> = emptyList(),
    val selectedTvName: String = "New TV",
    val pickerOpen: Boolean = false,
    val online: Boolean = false,
    val powerOn: Boolean = true,
    val muted: Boolean = false,
    val cursorMode: Boolean = false,
    val volumePercent: Int = 60,
    val errorMessage: String? = null
)

class RemoteViewModel(app: Application) : AndroidViewModel(app) {

    private val certManager = CertificateManager(app)
    private val remote = AndroidTvRemoteClient(certManager)
    private val pairingClient = AndroidTvPairingClient(certManager)
    private val discovery = TvDiscovery(app)

    private var pendingHost: String? = null

    // The pairing handshake (below) suspends right after the TV starts showing
    // its code, waiting on this to be completed with whatever the person types
    // into the dialog. This is what lets us keep the SAME pairing socket open
    // between "TV shows code" and "person typed it in" instead of guessing the
    // code before the TV has even generated one.
    private var codeDeferred: CompletableDeferred<String>? = null
    private var pairingJob: Job? = null

    // Android's volume stream has 15 discrete steps. Real remotes always move one
    // step at a time with a real VOLUME_UP/DOWN key press, which is what makes the
    // TV draw its native on-screen volume bar — so we mirror that here too.
    private var volumeStep: Int = 9 // ~60%, matches the default volumePercent below

    private val _uiState = MutableStateFlow(RemoteUiState())
    val uiState: StateFlow<RemoteUiState> = _uiState.asStateFlow()

    fun togglePicker() {
        _uiState.value = _uiState.value.copy(pickerOpen = !_uiState.value.pickerOpen)
        if (_uiState.value.pickerOpen) scanForTvs()
    }

    fun closePicker() {
        _uiState.value = _uiState.value.copy(pickerOpen = false)
    }

    private fun scanForTvs() {
        _uiState.value = _uiState.value.copy(connectionState = ConnectionState.SCANNING)
        viewModelScope.launch {
            val found = discovery.scan()
            _uiState.value = _uiState.value.copy(
                discoveredTvs = found,
                connectionState = if (remote.isConnected) ConnectionState.CONNECTED else ConnectionState.DISCONNECTED
            )
        }
    }

    fun connectTo(tv: DiscoveredTv) {
        pendingHost = tv.host
        _uiState.value = _uiState.value.copy(
            connectionState = ConnectionState.CONNECTING,
            selectedTvName = tv.name,
            pickerOpen = false
        )
        viewModelScope.launch { attemptConnect(tv.host) }
    }

    private suspend fun attemptConnect(host: String) {
        val result = remote.connect(host)
        if (result.isSuccess) {
            _uiState.value = _uiState.value.copy(
                connectionState = ConnectionState.CONNECTED,
                online = true,
                errorMessage = null
            )
        } else {
            // Most likely cause: this TV has never seen our certificate before.
            startPairing(host)
        }
    }

    /**
     * Opens the pairing socket and runs the handshake ourselves. The TV only
     * puts the code on screen once we reach the PairingConfiguration step
     * inside [AndroidTvPairingClient.pair] — so we must already be mid-handshake,
     * with that same socket held open, before we ever ask the person to type
     * anything. That's why the code is read via a suspending [CompletableDeferred]
     * instead of being collected from the dialog up front: the dialog is only
     * shown once the callback below actually runs, i.e. once the TV is already
     * displaying a fresh code.
     */
    private fun startPairing(host: String) {
        pendingHost = host
        pairingJob?.cancel()
        pairingJob = viewModelScope.launch {
            val result = pairingClient.pair(host) {
                val deferred = CompletableDeferred<String>()
                codeDeferred = deferred
                _uiState.value = _uiState.value.copy(
                    connectionState = ConnectionState.NEEDS_PAIRING_CODE,
                    errorMessage = null
                )
                deferred.await()
            }
            codeDeferred = null
            when (result) {
                is AndroidTvPairingClient.PairingResult.Success -> attemptConnect(host)
                is AndroidTvPairingClient.PairingResult.WrongCode -> _uiState.value = _uiState.value.copy(
                    connectionState = ConnectionState.ERROR,
                    errorMessage = "Code galat tha ya TV pe expire ho gaya. \"Try Again\" dabao — TV nayi screen dikhayega."
                )
                is AndroidTvPairingClient.PairingResult.Error -> _uiState.value = _uiState.value.copy(
                    connectionState = ConnectionState.ERROR,
                    errorMessage = result.message
                )
            }
        }
    }

    /** Called once the person has read the code that's currently on the TV screen and typed it in. */
    fun submitPairingCode(code: String) {
        _uiState.value = _uiState.value.copy(connectionState = ConnectionState.PAIRING)
        codeDeferred?.complete(code)
    }

    /**
     * After any pairing/connection failure: start over from the top (not just
     * re-open the pairing socket), so a plain connect error gets a real retry
     * too, not just a wrong-code retry.
     */
    fun retryPairing() {
        val host = pendingHost ?: return
        _uiState.value = _uiState.value.copy(connectionState = ConnectionState.CONNECTING, errorMessage = null)
        viewModelScope.launch { attemptConnect(host) }
    }

    fun cancelPairing() {
        pairingJob?.cancel()
        codeDeferred = null
        _uiState.value = _uiState.value.copy(connectionState = ConnectionState.DISCONNECTED, errorMessage = null)
    }

    fun togglePower() = act {
        remote.sendKey(RemoteKeyCode.KEYCODE_POWER)
        _uiState.value = _uiState.value.copy(powerOn = !_uiState.value.powerOn)
    }

    fun toggleMute() = act {
        remote.sendKey(RemoteKeyCode.KEYCODE_VOLUME_MUTE)
        _uiState.value = _uiState.value.copy(muted = !_uiState.value.muted)
    }

    fun toggleCursorMode() {
        _uiState.value = _uiState.value.copy(cursorMode = !_uiState.value.cursorMode)
    }

    /** Slider drag: move to the nearest step for the dragged percent, one real key per step. */
    fun setVolume(percent: Int) = act {
        val targetStep = ((percent.coerceIn(0, 100) * 15) + 50) / 100
        moveVolumeToStep(targetStep)
    }

    /** +/- buttons and the volume rocker: exactly one real step per tap. */
    fun stepVolume(deltaPercent: Int) = act {
        moveVolumeToStep(volumeStep + if (deltaPercent > 0) 1 else -1)
    }

    private suspend fun moveVolumeToStep(target: Int) {
        val clamped = target.coerceIn(0, 15)
        val diff = clamped - volumeStep
        if (diff > 0) repeat(diff) { remote.sendKey(RemoteKeyCode.KEYCODE_VOLUME_UP) }
        if (diff < 0) repeat(-diff) { remote.sendKey(RemoteKeyCode.KEYCODE_VOLUME_DOWN) }
        volumeStep = clamped
        _uiState.value = _uiState.value.copy(volumePercent = volumeStep * 100 / 15)
    }

    fun back() = act { remote.sendKey(RemoteKeyCode.KEYCODE_BACK) }
    fun home() = act { remote.sendKey(RemoteKeyCode.KEYCODE_HOME) }
    fun recentApps() = act { remote.sendKey(RemoteKeyCode.KEYCODE_APP_SWITCH) }
    fun assistant() = act { remote.sendKey(RemoteKeyCode.KEYCODE_VOICE_ASSIST) }
    fun menu() = act { remote.sendKey(RemoteKeyCode.KEYCODE_MENU) }
    fun openSettings() = act { remote.sendKey(RemoteKeyCode.KEYCODE_SETTINGS) }
    fun openKeyboard() = act { remote.sendKey(RemoteKeyCode.KEYCODE_DPAD_CENTER) }

    fun dpadTap() = act { remote.sendKey(RemoteKeyCode.KEYCODE_DPAD_CENTER) }
    fun dpadDirection(dx: Int, dy: Int) = act {
        val key = when {
            kotlin.math.abs(dx) > kotlin.math.abs(dy) && dx > 0 -> RemoteKeyCode.KEYCODE_DPAD_RIGHT
            kotlin.math.abs(dx) > kotlin.math.abs(dy) && dx < 0 -> RemoteKeyCode.KEYCODE_DPAD_LEFT
            dy > 0 -> RemoteKeyCode.KEYCODE_DPAD_DOWN
            dy < 0 -> RemoteKeyCode.KEYCODE_DPAD_UP
            else -> null
        }
        key?.let { remote.sendKey(it) }
    }

    // These 3 apps don't have a dedicated protocol key; the TV's own home
    // screen shows them as tiles once you're on it, so we just go Home.
    fun launchYoutube() = act { remote.sendKey(RemoteKeyCode.KEYCODE_HOME) }
    fun launchNetflix() = act { remote.sendKey(RemoteKeyCode.KEYCODE_HOME) }
    fun launchPrime() = act { remote.sendKey(RemoteKeyCode.KEYCODE_HOME) }

    /** Runs a remote action only if connected; silently no-ops otherwise (mirrors a real remote). */
    private fun act(block: suspend () -> Unit) {
        if (!remote.isConnected) return
        viewModelScope.launch { block() }
    }

    override fun onCleared() {
        super.onCleared()
        remote.close()
    }
}
