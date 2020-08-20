import '../styles/index.css'
import { WebSocketWrapper } from '../components/WebSocketWrapper'

export default function App({ Component, pageProps }) {
  return (
    <WebSocketWrapper>
      <Component {...pageProps} />
    </WebSocketWrapper>
  )
}
