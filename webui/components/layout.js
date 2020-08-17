import Head from 'next/head'
import styles from './layout.module.css'
import utilStyles from '../styles/utils.module.css'
import Link from 'next/link'
import {
  Button,
  Heading,
  Pane,
  Text,
  Link as EvergreenLink,
} from 'evergreen-ui'

const name = 'Your Name'
export const siteTitle = 'Next.js Sample Website'

export default function Layout({ children, home }) {
  return (
    <div className={styles.container}>
      <Head>
        <link rel="icon" href="/favicon.ico" />
        <meta
          name="description"
          content="Learn how to build a personal website using Next.js"
        />
        <meta
          property="og:image"
          content={`https://og-image.now.sh/${encodeURI(
            siteTitle
          )}.png?theme=light&md=0&fontSize=75px&images=https%3A%2F%2Fassets.vercel.com%2Fimage%2Fupload%2Ffront%2Fassets%2Fdesign%2Fnextjs-black-logo.svg`}
        />
        <meta name="og:title" content={siteTitle} />
        <meta name="twitter:card" content="summary_large_image" />
      </Head>
      {/* <header className={styles.header}>
        {home ? (
          <>
            <img
              src="/images/profile.png"
              className={`${styles.headerHomeImage} ${utilStyles.borderCircle}`}
              alt={name}
            />
            <h1 className={utilStyles.heading2Xl}>{name}</h1>
          </>
        ) : (
          <>
            <Link href="/">
              <a>
                <img
                  src="/images/profile.png"
                  className={`${styles.headerImage} ${utilStyles.borderCircle}`}
                  alt={name}
                />
              </a>
            </Link>
            <h2 className={utilStyles.headingLg}>
              <Link href="/">
                <a className={utilStyles.colorInherit}>{name}</a>
              </Link>
            </h2>
          </>
        )}
      </header>
      <main>{children}</main>
      {!home && (
        <div className={styles.backToHome}>
          <Link href="/">
            <a>← Back to home</a>
          </Link>
        </div>
      )} */}
      <Pane
        height={79}
        padding={16}
        display="flex"
        alignItems="center"
        justifyContent="center"
        border="default"
      >
        <Pane alignItems="center" display="flex">
          <img height={68} src="/images/panty-logo-text-sm-view.svg" />
        </Pane>
        <Pane flex={1} display="flex" justifyContent="center">
          {/* Below you can see the marginRight property on a Button. */}
          <EvergreenLink href="#" marginRight={12}>
            Home
          </EvergreenLink>
          <EvergreenLink href="#" marginRight={12}>
            Busy
          </EvergreenLink>
          <EvergreenLink href="#" marginRight={12}>
            Queues
          </EvergreenLink>
          <EvergreenLink href="#" marginRight={12}>
            Retries
          </EvergreenLink>
          <EvergreenLink href="#" marginRight={12}>
            Scheduled
          </EvergreenLink>
          <EvergreenLink href="#" marginRight={12}>
            Dead
          </EvergreenLink>
        </Pane>
        <Pane>
          {/* Below you can see the marginRight property on a Button. */}
          <Button>Idle</Button>
        </Pane>
      </Pane>
      <Pane
        display="flex"
        alignItems="center"
        justifyContent="center"
        border="none"
      >
        <main>{children}</main>
      </Pane>
    </div>
  )
}
