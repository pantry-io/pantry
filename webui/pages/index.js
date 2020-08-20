import React, { useState, useCallback, useMemo, useRef } from 'react'
import Head from 'next/head'
import Layout, { siteTitle } from '../components/layout'
import utilStyles from '../styles/utils.module.css'
import Link from 'next/link'
import Date from '../components/date'
import ResponsiveLine from '../components/charts/line'
import { ResponsiveLineData } from '../components/charts/line'
import {
  Button,
  Heading,
  Pane,
  Text,
  Link as EvergreenLink,
} from 'evergreen-ui'
import useGlobal from '../store'

const mapState = (state) => ({
  connectionStatus: state.websocket.readyState,
  statsMessages: state.statsMessages,
})

export default function Home() {
  const [state] = useGlobal(mapState)
  const { connectionStatus, statsMessages } = state

  return (
    <Layout home>
      <Head>
        <title>{siteTitle}</title>
      </Head>
      <Pane
        height={79}
        padding={16}
        display="flex"
        alignItems="center"
        justifyContent="space-around"
        border="default"
      >
        <Pane display="flex" flexDirection="column" alignItems="center">
          <Text size={600}>1,345,678</Text>
          <Text size={300}>Processed</Text>
        </Pane>
        <Pane display="flex" flexDirection="column" alignItems="center">
          <Text size={600}>10</Text>
          <Text size={300}>Instances</Text>
        </Pane>
        <Pane display="flex" flexDirection="column" alignItems="center">
          <Text size={600}>297,345,678</Text>
          <Text size={300}>Enqueued</Text>
        </Pane>
        <Pane display="flex" flexDirection="column" alignItems="center">
          <Text size={600}>1,789</Text>
          <Text size={300}>In Flight</Text>
        </Pane>
        <Pane display="flex" flexDirection="column" alignItems="center">
          <Text size={600}>8 ms</Text>
          <Text size={300}>Ack Latency p95</Text>
        </Pane>
        <Pane display="flex" flexDirection="column" alignItems="center">
          <Text size={600}>22 ms</Text>
          <Text size={300}>Ack Latency p99</Text>
        </Pane>
      </Pane>
      <Pane>
        <div>
          <span>The WebSocket is currently {connectionStatus}</span>
          <ul>
            {statsMessages.map((message, idx) => (
              <span key={idx}>{message}</span>
            ))}
          </ul>
        </div>
      </Pane>
      <Pane height={500}>
        {/* <ResponsiveLine data={ResponsiveLineData} /> */}
      </Pane>

      {/* <section className={utilStyles.headingMd}>
        <p>[Your Self Introduction]</p>
        <p>
          (This is a sample website - you’ll be building a site like this on{' '}
          <a href="https://nextjs.org/learn">our Next.js tutorial</a>.)
        </p>
      </section>

      <section className={utilStyles.headingMd}>…</section>
      <section className={`${utilStyles.headingMd} ${utilStyles.padding1px}`}>
        <h2 className={utilStyles.headingLg}>Blog</h2>
        <ul className={utilStyles.list}>
          {allPostsData.map(({ id, date, title }) => (
            <li className={utilStyles.listItem} key={id}>
              <Link href="/posts/[id]" as={`/posts/${id}`}>
                <a>{title}</a>
              </Link>
              <br />
              <small className={utilStyles.lightText}>
                <Date dateString={date} />
              </small>
            </li>
          ))}
        </ul>
      </section> */}
    </Layout>
  )
}
