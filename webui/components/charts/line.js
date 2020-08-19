import { ResponsiveLine } from '@nivo/line'
// make sure parent container have a defined height when using
// responsive component, otherwise height will be 0 and
// no chart will be rendered.
// website examples showcase many properties,
// you'll often use just a few of them.
const MyResponsiveLine = ({ data /* see data tab */ }) => (
  <ResponsiveLine
    data={data}
    margin={{ top: 50, right: 110, bottom: 50, left: 60 }}
    xScale={{ type: 'point' }}
    yScale={{
      type: 'linear',
      min: 'auto',
      max: 'auto',
      stacked: false,
      reverse: false,
    }}
    curve="monotoneX"
    axisTop={null}
    axisRight={null}
    axisBottom={{
      orient: 'bottom',
      tickSize: 5,
      tickPadding: 5,
      tickRotation: -44,
      legend: 'time',
      legendOffset: 36,
      legendPosition: 'middle',
    }}
    axisLeft={{
      orient: 'left',
      tickSize: 5,
      tickPadding: 5,
      tickRotation: 0,
      legend: 'count',
      legendOffset: -40,
      legendPosition: 'middle',
    }}
    colors={{ scheme: 'spectral' }}
    lineWidth={1}
    enablePoints={false}
    pointSize={10}
    pointColor={{ theme: 'background' }}
    pointBorderWidth={2}
    pointBorderColor={{ from: 'serieColor' }}
    pointLabel="y"
    pointLabelYOffset={-12}
    useMesh={true}
    legends={[
      {
        anchor: 'bottom-right',
        direction: 'column',
        justify: false,
        translateX: 100,
        translateY: 0,
        itemsSpacing: 0,
        itemDirection: 'left-to-right',
        itemWidth: 80,
        itemHeight: 20,
        itemOpacity: 0.75,
        symbolSize: 12,
        symbolShape: 'circle',
        symbolBorderColor: 'rgba(0, 0, 0, .5)',
        effects: [
          {
            on: 'hover',
            style: {
              itemBackground: 'rgba(0, 0, 0, .03)',
              itemOpacity: 1,
            },
          },
        ],
      },
    ]}
  />
)

export default MyResponsiveLine

export const ResponsiveLineData = [
  {
    id: 'japan',
    color: 'hsl(116, 70%, 50%)',
    data: [
      {
        x: 'plane',
        y: 27,
      },
      {
        x: 'helicopter',
        y: 107,
      },
      {
        x: 'boat',
        y: 246,
      },
      {
        x: 'train',
        y: 166,
      },
      {
        x: 'subway',
        y: 206,
      },
      {
        x: 'bus',
        y: 136,
      },
      {
        x: 'car',
        y: 129,
      },
      {
        x: 'moto',
        y: 179,
      },
      {
        x: 'bicycle',
        y: 175,
      },
      {
        x: 'horse',
        y: 115,
      },
      {
        x: 'skateboard',
        y: 4,
      },
      {
        x: 'others',
        y: 0,
      },
    ],
  },
  {
    id: 'france',
    color: 'hsl(39, 70%, 50%)',
    data: [
      {
        x: 'plane',
        y: 182,
      },
      {
        x: 'helicopter',
        y: 299,
      },
      {
        x: 'boat',
        y: 251,
      },
      {
        x: 'train',
        y: 179,
      },
      {
        x: 'subway',
        y: 11,
      },
      {
        x: 'bus',
        y: 206,
      },
      {
        x: 'car',
        y: 290,
      },
      {
        x: 'moto',
        y: 75,
      },
      {
        x: 'bicycle',
        y: 75,
      },
      {
        x: 'horse',
        y: 178,
      },
      {
        x: 'skateboard',
        y: 41,
      },
      {
        x: 'others',
        y: 238,
      },
    ],
  },
  {
    id: 'us',
    color: 'hsl(48, 70%, 50%)',
    data: [
      {
        x: 'plane',
        y: 294,
      },
      {
        x: 'helicopter',
        y: 74,
      },
      {
        x: 'boat',
        y: 11,
      },
      {
        x: 'train',
        y: 265,
      },
      {
        x: 'subway',
        y: 38,
      },
      {
        x: 'bus',
        y: 112,
      },
      {
        x: 'car',
        y: 79,
      },
      {
        x: 'moto',
        y: 26,
      },
      {
        x: 'bicycle',
        y: 272,
      },
      {
        x: 'horse',
        y: 145,
      },
      {
        x: 'skateboard',
        y: 11,
      },
      {
        x: 'others',
        y: 241,
      },
    ],
  },
  {
    id: 'germany',
    color: 'hsl(183, 70%, 50%)',
    data: [
      {
        x: 'plane',
        y: 1,
      },
      {
        x: 'helicopter',
        y: 29,
      },
      {
        x: 'boat',
        y: 122,
      },
      {
        x: 'train',
        y: 204,
      },
      {
        x: 'subway',
        y: 11,
      },
      {
        x: 'bus',
        y: 209,
      },
      {
        x: 'car',
        y: 288,
      },
      {
        x: 'moto',
        y: 46,
      },
      {
        x: 'bicycle',
        y: 35,
      },
      {
        x: 'horse',
        y: 275,
      },
      {
        x: 'skateboard',
        y: 52,
      },
      {
        x: 'others',
        y: 15,
      },
    ],
  },
  {
    id: 'norway',
    color: 'hsl(150, 70%, 50%)',
    data: [
      {
        x: 'plane',
        y: 188,
      },
      {
        x: 'helicopter',
        y: 84,
      },
      {
        x: 'boat',
        y: 202,
      },
      {
        x: 'train',
        y: 195,
      },
      {
        x: 'subway',
        y: 191,
      },
      {
        x: 'bus',
        y: 10,
      },
      {
        x: 'car',
        y: 291,
      },
      {
        x: 'moto',
        y: 153,
      },
      {
        x: 'bicycle',
        y: 76,
      },
      {
        x: 'horse',
        y: 92,
      },
      {
        x: 'skateboard',
        y: 141,
      },
      {
        x: 'others',
        y: 92,
      },
    ],
  },
]
