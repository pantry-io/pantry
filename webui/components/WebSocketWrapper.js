import React, { useState, useCallback, useMemo, useRef, useEffect } from 'react'
import useWebSocket, { ReadyState } from 'react-use-websocket'
import useGlobal from '../store'

const CommandKey = 'c'
const InvalidCommand = 0
const StatsSubscribe = 1
const StatsMessage = 2

const mapActions = (actions) => {
  return {
    addStatsMessage: actions.stats.addMessage,
    setWsApiReadyState: actions.wsapi.setReadyState,
  }
}

export const WebSocketWrapper = (props) => {
  const [, actions] = useGlobal(null, mapActions)
  // Public API that will echo messages sent to it back to the client
  // TODO: We'll need to set this based on dotenv
  const [socketUrl, setSocketUrl] = useState('ws://heat.ts:8080/ws')
  const {
    sendMessage,
    sendJsonMessage,
    lastMessage,
    lastJsonMessage,
    readyState,
    getWebSocket,
  } = useWebSocket(socketUrl, {
    onOpen: () => console.log('opened'),
    // Will attempt to reconnect on all close events, such as server shutting down
    shouldReconnect: (closeEvent) => true,
  })

  useEffect(() => {
    if (lastMessage != null) {
      protocolHandler(lastMessage)
    }
  }, [lastMessage])

  const protocolHandler = (msg) => {
    let m = null
    try {
      m = JSON.parse(msg.data)
    } catch (err) {
      console.log('Unable to parse wsapi message', err, msg)
      return
    }

    const cmd = m[CommandKey]
    switch (cmd) {
      case StatsMessage:
        actions.addStatsMessage(m)
      default:
        console.log('unknown wsapi command: ', cmd)
    }
  }

  const handleClickSendMessage = useCallback(() => sendMessage('Hello'), [])

  const connectionStatus = {
    [ReadyState.CONNECTING]: 'Connecting',
    [ReadyState.OPEN]: 'Open',
    [ReadyState.CLOSING]: 'Closing',
    [ReadyState.CLOSED]: 'Closed',
    [ReadyState.UNINSTANTIATED]: 'Uninstantiated',
  }[readyState]

  useEffect(() => {
    actions.setWsApiReadyState(connectionStatus)
  }, [connectionStatus])

  return (
    <div>
      <button
        onClick={handleClickSendMessage}
        disabled={readyState !== ReadyState.OPEN}
      >
        Click Me to send 'Hello'
      </button>
      <span>The WebSocket is currently {connectionStatus}</span>
      {!!lastMessage ? (
        <span>Last message: {lastMessage.data}</span>
      ) : (
        <div></div>
      )}
      {props.children}
    </div>
  )
}
