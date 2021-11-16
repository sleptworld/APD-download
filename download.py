import os
try:
    from rich.live import Live
    from rich.table import Table
    from rich.columns import Columns
    import asyncio
    import argparse
    from rich import console
    from rich.console import Console, Group, group
    from rich.padding import Padding, PaddingDimensions
    from rich.text import Text
    from rich.panel import Panel
    from rich.progress import BarColumn, Progress, TimeRemainingColumn
    from datetime import datetime, timedelta
    import aiohttp

except Exception:
    os.system('pythom -m pip install -U rich,aiohttp')

TITLE = '  \
     ____  __  ______  __  _____  __\n  \
    / __ \/ / / / __ \/  |/  / / / /\n \
   / /_/ / / / / / / / /|_/ / / / / \n  \
 / _, _/ /_/ / /_/ / /  / / /_/ /  \n\
/_/ |_|\____/\____/_/  /_/\____/   \n\
'

URLS = [
    'https://quotsoft.net/air/data/china_sites_{}.csv',
    'https://quotsoft.net/air/data/china_cities_{}.csv',
    'https://quotsoft.net/air/data/beijing_all_{}.csv',
    'https://quotsoft.net/air/data/beijing_extra_{}.csv'
]

DES = "\
空气质量数据类型包括PM2.5, PM10, SO2, NO2, O3, CO, AQI。\n\
\n\
全国空气质量数据来自中国环境监测总站的全国城市空气质量实时发布平台，每日更新。\n\
\n\
北京市空气质量数据来自北京市环境保护检测中心网站，每日更新。\n\
\n\
气象数据要素包括气温、气压、露点、风向风速、云量、降水量。\n\
\n\
气象数据来自美国国家气候数据中心（NCDC），每年不定期更新。\n\
\n\
"

progress = Progress(
    "[progress.description]{task.description}",
    BarColumn(),
    "[progress.percentage]{task.percentage:>3.0f}%",
    TimeRemainingColumn(),
    expand=True,
    transient=True,
)

allProgress = Progress("[progress.description]{task.description}",
                       BarColumn(),
                       "[progress.percentage]{task.percentage:>3.0f}%",
                       TimeRemainingColumn(),
                       expand=True)

header = Group(
    Text(TITLE, style="bold blink green", justify="center"),
    Text(datetime.now().strftime('%Y-%m-%d'),
         justify="center",
         style="bold blink"),
    Padding(Text(DES, justify="center"), (3, 0, 0, 0)),
)

failedList = []

failedColumn = Columns(
	failedList,
	
)

pgBarTable = Table.grid()

pgBarTable.add_row(
    Panel.fit(allProgress,
              title="Overall Progress",
              border_style="green",
              padding=(2, 2)),
    Panel.fit(progress, title="[b]Jobs", border_style="red", padding=(1, 2)),
	Panel.fit(failedColumn,title=":warning: 错误列表",border_style="red",padding=(2,2)),
)

session = aiohttp.ClientSession()

sem = asyncio.Semaphore(10)


def getNotice(dp: str, te: str, datespan) -> Panel:

    t = Text()

    t.append(("数据类型：{}\n").format(te), style="blue")
    t.append(("下载路径：{}\n").format(dp))
    t.append(("时间范围：{} - {}\n").format(datespan[0].strftime('%Y-%m-%d'),
                                       datespan[1].strftime('%Y-%m-%d')),
             style="bold red")

    return Panel(t, title=":loudspeaker: 变量设置", subtitle="rmrmrmrm")


console = Console()


async def sDownload(t: str, url: str, dp: str, hd):

    try:
        async with sem:
            async with (session.get(url.format(t))) as res:

                if res.status == 200:
                    chunk_size = 2048
                    content_size = int(res.headers['content-length'])
                    dn = 0
                    handler = progress.add_task(
                        ("[green]{}").format(t),
                        total=content_size,
                    )

                    with open(os.path.join(dp, t + '.csv'), 'wb') as f:

                        async for data in res.content.iter_chunked(chunk_size):
                            f.write(data)
                            dn += len(data)
                            if not progress.finished:
                                progress.update(handler, advance=len(data))
                            if dn >= content_size:
                                progress.update(handler, visible=False)
                                allProgress.update(hd, advance=1)
                else:
                    allProgress.update(hd, advance=1)

    except Exception:
        # console.print_exception(show_locals=True)
        failedList.append(t)


async def download(s: datetime, e: datetime, url: str, dp: str):

    ds = (e - s).days
    hd = allProgress.add_task("All", total=ds)
    tasks = []
    while s < e:
        tasks.append(
            asyncio.create_task(
                sDownload(s.strftime('%Y%m%d'), url=url, dp=dp, hd=hd)))
        s += timedelta(days=1)

    async with session:
        await asyncio.gather(*tasks)


def timeCheck(t: str):
    err_msg = '不是有效值'
    try:
        tmp = t.split('-')

        if len(tmp) != 2:
            raise argparse.ArgumentTypeError

        return (datetime.strptime(tmp[0], '%Y%m%d'),
                datetime.strptime(tmp[1], '%Y%m%d'))

    except ValueError:
        raise argparse.ArgumentTypeError(err_msg)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--type',
        dest='type',
        type=int,
        nargs=1,
        help=
        '下载的数据类型，默认类型2：\n 1. 全国国控监测点数据 CSV格式 \n 2.全国城市数据 CSV格式 \n 3. 北京PM2.5/PM10/AQI数据 CSV格式 \n 4. 北京SO2/NO2/O3/CO数据 CSV格式\n',
        default=1)

    parser.add_argument('-d',
                        dest='downloadPath',
                        help='下载目录，默认当前目录下的data文件夹',
                        type=str,
                        nargs=1,
                        default='./data/')

    parser.add_argument('-t',
                        dest='time',
                        type=timeCheck,
                        help='数据时间范围，默认2015-01-01 至 2020-12-31',
                        default='20150101-20201231',
                        metavar='20150101-20201231')

    args = parser.parse_args()
    downloadPath = args.downloadPath
    t = args.time
    u = URLS[args.type - 1]

    if not os.path.exists(downloadPath):
        os.makedirs(downloadPath)

    with Live(pgBarTable, refresh_per_second=10):
        notice = getNotice(downloadPath, u, t)
        console.print(header, notice, pgBarTable)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(
            download(s=t[0], e=t[1], url=u, dp=downloadPath))


if __name__ == '__main__':
    main()
