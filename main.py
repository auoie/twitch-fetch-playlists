from hashlib import sha1
from pathlib import Path
from time import sleep
from typing import List, Literal
from curl_cffi import requests as cf_requests
import dateutil
import requests
import dateutil.parser
import m3u8
from pydantic import BaseModel
from pydantic_core import ValidationError
from typed_argparse import TypedArgs
import typed_argparse

from generated import api, pageinfo

DOMAINS = [
    "https://vod-secure.twitch.tv/",
    "https://vod-metro.twitch.tv/",
    "https://vod-pop-secure.twitch.tv/",
    "https://d1m7jfoe9zdc1j.cloudfront.net/",
    "https://d1mhjrowxxagfy.cloudfront.net/",
    "https://d1ymi26ma8va5x.cloudfront.net/",
    "https://d2nvs31859zcd8.cloudfront.net/",
    "https://d2vjef5jvl6bfs.cloudfront.net/",
    "https://d3vd9lfkzbru3h.cloudfront.net/",
    "https://dgeft87wbj63p.cloudfront.net/",
    "https://dqrpb9wgowsf5.cloudfront.net/",
    "https://ds0h3roq6wcgc.cloudfront.net/",
]

T_RESOLUTION = Literal["chunked", "720p60", "720p30", "480p30", "360p30", "160p30"]


class Arguments(TypedArgs):
    streamer_name: str = typed_argparse.arg(
        "-s", help="streamer login name, all lower case no spaces"
    )
    resolution: T_RESOLUTION = typed_argparse.arg(
        "-r", help="resolution (chunked is highest resolution)", default="chunked"
    )


class M3U8Stream(BaseModel):
    link: str
    content: str
    partial_link: str
    path: str


def get_valid_playlist(path: str, resolution: T_RESOLUTION) -> M3U8Stream | None:
    for domain in DOMAINS:
        link = f"{domain}{path}/{resolution}/index-dvr.m3u8"
        partial_link = f"{domain}{path}/{resolution}/"
        resp = requests.get(link, timeout=5)
        if resp.ok:
            return M3U8Stream(
                content=resp.text, link=link, partial_link=partial_link, path=path
            )
    return None


def make_sullygnome_link(streamer_name: str) -> str:
    return f"https://sullygnome.com/channel/{streamer_name}/"


def sullygnome_streams_link(streamer_id: int) -> str:
    return f"https://sullygnome.com/api/tables/channeltables/streams/90/{streamer_id}/%20/1/1/desc/0/100"


def fetch_behind_cloudflare(link: str) -> str:
    for _ in range(5):
        resp = cf_requests.get(link, impersonate="chrome", timeout=5)
        if not resp.ok:
            print(resp.status_code)
            sleep(5)
        else:
            return resp.text
    exit(1)


def replace_unmuted(path: str) -> str:
    return path.replace("unmuted", "muted")


def run_program(args: Arguments) -> None:
    streamer_name = args.streamer_name
    link = make_sullygnome_link(streamer_name)
    print("fetching:", link)
    page_content = fetch_behind_cloudflare(link)
    pageinfo_idx = page_content.find("var PageInfo")
    after_idx_content = page_content[pageinfo_idx:]
    pageinfo_json = after_idx_content[
        after_idx_content.find("{") : after_idx_content.find(";")
    ]
    try:
        id_obj = pageinfo.Model.model_validate_json(pageinfo_json)
    except ValidationError as err:
        print(err)
        exit(1)
    print("sullygnome id:", id_obj.id)
    apiLink = sullygnome_streams_link(id_obj.id)
    print("fetching:", apiLink)
    # note: sullygnome might miss some streams. double-check with streamscharts for completeness
    sullyGnomeApiContent = fetch_behind_cloudflare(apiLink)
    try:
        streams_api_response = api.Model.model_validate_json(sullyGnomeApiContent)
    except ValidationError as err:
        print(err)
        exit(1)
    paths: List[tuple[str, api.Datum]] = []
    for stream_data in streams_api_response.data:
        # note: could be off by 1 second
        unix_timestamp = int(
            dateutil.parser.isoparse(stream_data.startDateTime).timestamp()
        )
        input_str = f"{stream_data.channelurl}_{stream_data.streamId}_{unix_timestamp}"
        hash = sha1(input_str.encode("utf-8")).hexdigest()
        path = f"{hash[:20]}_{input_str}"
        paths.append((path, stream_data))
    print(f"{len(paths)} streams to consider")
    for path in paths:
        stream_data = path[1]
        response = get_valid_playlist(path[0], args.resolution)
        if response is not None:
            print("found:", response.link)
            content = m3u8.loads(response.content)
            for untypedSegment in content.segments:
                segment: m3u8.Segment = untypedSegment
                # note: possible for some segments to be gone
                if not isinstance(segment.uri, str):
                    print(f"err: {segment} in {response.link} has a non-string uri")
                    exit(1)
                segment.uri = f"{response.partial_link}{replace_unmuted(segment.uri)}"
            new_file: str = content.dumps()
            streamer_folder = Path(
                "playlists", streamer_name, args.resolution
            ).resolve()
            streamer_folder.mkdir(parents=True, exist_ok=True)
            filePath = Path(
                streamer_folder,
                f"{stream_data.channelurl}_{stream_data.startDateTime}_{stream_data.streamId}_index.m3u8",
            ).resolve()
            filePath.write_text(new_file)
        else:
            print("not found:", path)


def main():
    typed_argparse.Parser(Arguments).bind(run_program).run()


if __name__ == "__main__":
    main()
