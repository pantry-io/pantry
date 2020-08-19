import React, { useState, useCallback, useMemo, useRef, useEffect } from 'react'
import useWebSocket, { ReadyState } from 'react-use-websocket'

export const WebSocketDemo = () => {
  //Public API that will echo messages sent to it back to the client
  const [socketUrl, setSocketUrl] = useState('wss://echo.websocket.org')
  const {
    sendMessage,
    sendJsonMessage,
    lastMessage,
    lastJsonMessage,
    readyState,
    getWebSocket,
  } = useWebSocket(socketUrl, {
    onOpen: () => console.log('opened'),
    //Will attempt to reconnect on all close events, such as server shutting down
    shouldReconnect: (closeEvent) => true,
  })

  // const messageHistory = useRef([])
  const [messageHistory, setMessageHistory] = useState([])
  useEffect(() => {
    if (lastMessage != null) {
      setMessageHistory(messageHistory.concat(lastMessage))
    }
  }, [lastMessage])

  // messageHistory.current = useMemo(() => {
  //   if (lastMessage != null) {
  //     messageHistory.current.concat(lastMessage)
  //   }
  // }, [lastMessage])

  const handleClickChangeSocketUrl = useCallback(
    () => setSocketUrl('wss://demos.kaazing.com/echo'),
    []
  )

  const handleClickSendMessage = useCallback(() => sendMessage('Hello'), [])

  const connectionStatus = {
    [ReadyState.CONNECTING]: 'Connecting',
    [ReadyState.OPEN]: 'Open',
    [ReadyState.CLOSING]: 'Closing',
    [ReadyState.CLOSED]: 'Closed',
    [ReadyState.UNINSTANTIATED]: 'Uninstantiated',
  }[readyState]

  // if (!lastMessage || !lastMessage.data) {
  //   return <div>Not ready</div>
  // }

  return (
    <div>
      <button onClick={handleClickChangeSocketUrl}>
        Click Me to change Socket Url
      </button>
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
      <ul>
        {messageHistory &&
          messageHistory.current &&
          messageHistory.current.map((message, idx) => (
            <span key={idx}>{message.data}</span>
          ))}
      </ul>
    </div>
  )
}
