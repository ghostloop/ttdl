import os
import re
import json
import time
import itertools
import asyncio
import requests
import aiofiles
import aiohttp
import sqlite3
import argparse

from pathlib import Path
from tqdm.asyncio import tqdm
from datetime import datetime, timezone
from tqdm.asyncio import tqdm as tqdm_asyncio
from typing import Optional, List, Dict, Tuple, Any, Union
from playwright.sync_api import sync_playwright, Error as PlaywrightError


#########################################################
#                   Database Manager                    #
#########################################################


class DatabaseManager:
    def __init__(self, db_path: Union[str, Path]):
        self.db_path = str(db_path)
        self._init_db()

    def _init_db(self):
        """Create tables if they don't exist."""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()

        c.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY AUTOINCREMENT,
                unique_name TEXT NOT NULL,
                secuid TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                last_scrape DATETIME
            );
            """
        )

        c.execute(
            """
            CREATE TABLE IF NOT EXISTS aliases (
                alias_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                alias_name TEXT NOT NULL,
                FOREIGN KEY(user_id) REFERENCES users(user_id)
                    ON DELETE CASCADE
            );
            """
        )

        c.execute(
            """
            CREATE TABLE IF NOT EXISTS videos (
                video_id TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL,
                status TEXT NOT NULL,        -- 'complete' / 'failed' / 'pending'
                downloaded_at DATETIME,
                local_path TEXT,
                FOREIGN KEY(user_id) REFERENCES users(user_id)
                    ON DELETE CASCADE
            );
            """
        )

        conn.commit()
        conn.close()

    def get_user_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute("SELECT * FROM users WHERE unique_name = ?", (name,))
        row = c.fetchone()
        conn.close()

        if row:
            return {
                "user_id": row[0],
                "unique_name": row[1],
                "secuid": row[2],
                "created_at": row[3],
                "last_scrape": row[4],
            }
        return None

    def create_user(self, name: str, secuid: str = "") -> int:
        """Inserts a new user row."""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute(
            "INSERT INTO users (unique_name, secuid) VALUES (?, ?)", (name, secuid)
        )
        user_id = c.lastrowid
        conn.commit()
        conn.close()
        return user_id

    def get_all_users(self) -> List[Dict[str, Any]]:
        """Returns all user rows as dicts."""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute("SELECT * FROM users")
        rows = c.fetchall()
        conn.close()

        results = []
        for row in rows:
            results.append(
                {
                    "user_id": row[0],
                    "unique_name": row[1],
                    "secuid": row[2],
                    "created_at": row[3],
                    "last_scrape": row[4],
                }
            )
        return results

    def update_last_scrape(self, user_id: int):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute(
            "UPDATE users SET last_scrape = CURRENT_TIMESTAMP WHERE user_id = ?",
            (user_id,),
        )
        conn.commit()
        conn.close()

    def add_alias(self, user_id: int, alias: str):
        """Insert an alias if not already present."""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute(
            """
            SELECT COUNT(*) FROM aliases
            WHERE user_id = ? AND alias_name = ?
            """,
            (user_id, alias),
        )
        (count,) = c.fetchone()
        if count == 0:
            c.execute(
                "INSERT INTO aliases (user_id, alias_name) VALUES (?, ?)",
                (user_id, alias),
            )
        conn.commit()
        conn.close()

    def get_video_status(self, video_id: str) -> Optional[str]:
        """Returns the status string ('complete', 'failed', etc.) or None if it doesn't exist."""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute("SELECT status FROM videos WHERE video_id = ?", (video_id,))
        row = c.fetchone()
        conn.close()
        return row[0] if row else None

    def set_video_status(
        self, video_id: str, user_id: int, status: str, local_path: Optional[str] = None
    ):
        """Insert or update a video's status row."""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()

        c.execute("SELECT video_id FROM videos WHERE video_id = ?", (video_id,))
        existing = c.fetchone()

        if not existing:
            c.execute(
                """
                INSERT INTO videos (video_id, user_id, status, local_path, downloaded_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (video_id, user_id, status, local_path),
            )
        else:
            c.execute(
                """
                UPDATE videos
                SET status = ?, downloaded_at = CURRENT_TIMESTAMP, local_path = ?
                WHERE video_id = ?
                """,
                (status, local_path, video_id),
            )

        conn.commit()
        conn.close()


#########################################################
#             Configuration + Main Scraper              #
#########################################################


class TikTokScraperConfig:
    """
    Configuration dataclass-like struct. Holds relevant config values.
    You can still override these if using this script as a module,
    but by default we always store downloads in script_dir/downloads.
    """

    def __init__(
        self,
        home_dir: Optional[Union[str, Path]] = None,
        max_retries: int = 10,
        print_outputs: bool = True,
        retry_failed: bool = False,
    ):
        if home_dir is None:
            self.home_dir = Path(__file__).parent / "downloads"
        else:
            self.home_dir = Path(home_dir)

        self.max_retries = max_retries
        self.print_outputs = print_outputs
        self.retry_failed = retry_failed


class TikTokScraper:
    """
    Main TikTokScraper class to handle:
      - Listing users from DB
      - For each user, run a TikTokDL instance
    """

    def __init__(self, config: TikTokScraperConfig, db: DatabaseManager) -> None:
        self.config = config
        self.db = db

        self.config.home_dir.mkdir(parents=True, exist_ok=True)

    def run_all_users(self) -> None:
        """Scrape all users from the DB."""
        users = self.db.get_all_users()
        total_users = len(users)
        for idx, user_row in enumerate(users, start=1):
            username = user_row["unique_name"]
            if self.config.print_outputs:
                print(f"Scraping profile {idx}/{total_users}: {username}")

            tiktok_dl = TikTokDL(
                user_name=username,
                user_id=user_row["user_id"],
                secuid=user_row["secuid"],
                home_dir=self.config.home_dir,
                print_outputs=self.config.print_outputs,
                retry_failed=self.config.retry_failed,
                max_retries=self.config.max_retries,
                db=self.db,
            )
            complete = tiktok_dl.scrape_profile()
            if complete:
                self.db.update_last_scrape(user_row["user_id"])
                if self.config.print_outputs:
                    print(f"Completed {username}")
            else:
                if self.config.print_outputs:
                    print(f"Skipping {username}")

    def run_single_user(self, username: str) -> None:
        """Scrape a single user from the DB by name."""
        user_row = self.db.get_user_by_name(username)
        if not user_row:
            print(f"User {username} not found in database.")
            return

        tiktok_dl = TikTokDL(
            user_name=user_row["unique_name"],
            user_id=user_row["user_id"],
            secuid=user_row["secuid"],
            home_dir=self.config.home_dir,
            print_outputs=self.config.print_outputs,
            retry_failed=self.config.retry_failed,
            max_retries=self.config.max_retries,
            db=self.db,
        )
        complete = tiktok_dl.scrape_profile()
        if complete:
            self.db.update_last_scrape(user_row["user_id"])
            if self.config.print_outputs:
                print(f"Completed {username}")
        else:
            if self.config.print_outputs:
                print(f"Skipping {username}")


#########################################################
#                    TikTokDL Class                     #
#########################################################


class TikTokDL:
    """
    Handles scraping/downloading for a single TikTok username.
    Replaces old metadata files with an SQLite DB approach.
    """

    def __init__(
        self,
        user_name: str,
        user_id: int,
        secuid: Optional[str],
        home_dir: Union[str, Path],
        print_outputs: bool,
        retry_failed: bool,
        max_retries: int,
        db: DatabaseManager,
    ) -> None:
        self.original_user_name = user_name
        self.user_id = user_id
        self.secuid = secuid or ""
        self.home_dir = Path(home_dir)
        self.print_outputs = print_outputs
        self.retry_failed = retry_failed
        self.max_retries = max_retries
        self.db = db

        # Additional config-like attributes
        self.save_with_date = False
        self.check_file_exists = False
        self.early_check_file_exists = False
        self.max_connections = 8
        self.timeout_per_file = 2
        self.print_scrape_date = True
        self.max_secuid_extraction_attempts = 10

        self.files_saved = 0

        self.video_test_string = "mime_type=video_mp4"
        self.date_format = "%Y-%m-%d_%H-%M-%S"

        self.tikwm_video_url_template = (
            "https://www.tikwm.com/video/media/hdplay/{video_id}.mp4"
        )
        self.tikwm_api_user_posts_url = "https://www.tikwm.com/api/user/posts"
        self.tikwm_api_video_url = "https://www.tikwm.com/api"

        self.tikwm_api_sleep_time = 1
        self.asyncio_error_sleep_time = 1

        self.loop: Optional[asyncio.AbstractEventLoop] = None

        # Derived placeholders
        self.user_folder: Path = self.home_dir.joinpath(self.original_user_name)
        self.user_name: str = self.original_user_name
        self.tiktok_user_link: str = ""
        self.tiktok_embed_link: str = ""

    def scrape_profile(self) -> bool:
        """Orchestrates the entire scraping process for a single user."""
        self._ifprinter(f"Scraping {self.original_user_name}")

        if self.print_scrape_date:
            current_date = datetime.now().strftime(self.date_format)
            self._ifprinter(f"Scraping started at {current_date}")

        self.user_folder.mkdir(parents=True, exist_ok=True)

        # If we don’t have a secuid in DB, try to get it
        if not self.secuid:
            self.secuid = self._extract_secuid_once(
                f"https://www.tiktok.com/@{self.user_name}", url_type="user"
            )
            if self.secuid is None:
                self._ifprinter("Could not find Secondary User ID.")
                return False
            self._update_user_secuid(self.secuid)

        # Possibly re-check if user renamed?
        new_name = self._get_user_name_from_secuid()
        if new_name and new_name != self.user_name:
            self._ifprinter(
                f"Secondary ID indicates {self.original_user_name} was renamed to {new_name}"
            )
            self.user_name = new_name
            self._add_alias(new_name)

        # Possibly re-assign user_folder based on new_name
        self.user_folder = self.home_dir.joinpath(self.user_name)
        self.user_folder.mkdir(parents=True, exist_ok=True)

        self._get_tiktok_links(self.user_name)

        # If we want to re-try previously failed items
        if self.retry_failed:
            failed_videos = self._get_videos_by_status("failed")
            if failed_videos:
                self._ifprinter(f"Retrying failed videos: {len(failed_videos)} total.")
                self._download_everything(
                    failed_videos, print_number_of_files_saved=False
                )

        # Attempt to get *latest* IDs from embed
        latest_video_id_list = self._get_latest_video_id_list()
        if latest_video_id_list is None or not latest_video_id_list:
            self._ifprinter("No new videos found from embed approach, using fallback.")
            return False

        # If the newest is already in complete or failed, probably no new videos
        newest_id = latest_video_id_list[0]
        if self._check_complete_or_failed(newest_id):
            self._ifprinter(f"No new videos for {self.user_name}")
            return True

        # If we haven't recognized the newest yet, check the list for recognized IDs:
        early_video_id_list = []
        download_now = False
        for vid_id in latest_video_id_list:
            if self._check_complete_or_failed(vid_id):
                download_now = True
            else:
                early_video_id_list.append(vid_id)

        if download_now:
            self._ifprinter(f"Downloading from embed video IDs for {self.user_name}.")
            self._download_everything(
                early_video_id_list, print_number_of_files_saved=True
            )
            return True

        # If none of them are recognized, we do a big fetch from TikWM
        self._ifprinter(f"Finding full video ID list for {self.user_name}")
        video_id_list = self._get_video_id_list()
        self._ifprinter(f"Found {len(video_id_list)} new video IDs.")

        if not video_id_list:
            self._ifprinter("Failed to find any video ID list from TikWM.")
            return False

        self._download_everything(video_id_list, print_number_of_files_saved=True)
        return True

    def _ifprinter(self, msg: str, end: Optional[str] = None) -> None:
        """Utility printing wrapper."""
        if self.print_outputs:
            print(msg, end=end)

    #########################################################
    #                  Database Helpers                     #
    #########################################################

    def _update_user_secuid(self, secuid: str):
        """Store the new secuid in the DB."""
        conn = sqlite3.connect(self.db.db_path)
        c = conn.cursor()
        c.execute("UPDATE users SET secuid=? WHERE user_id=?", (secuid, self.user_id))
        conn.commit()
        conn.close()

    def _add_alias(self, new_name: str):
        """Add new_name as an alias for this user."""
        self.db.add_alias(self.user_id, new_name)

    def _check_complete_or_failed(self, video_id: str) -> bool:
        """Return True if video_id is in DB with status=complete or failed."""
        status = self.db.get_video_status(video_id)
        return bool(status and status in ("complete", "failed"))

    def _get_videos_by_status(self, status_val: str) -> List[str]:
        """Return all video_ids for this user that match a specific status."""
        conn = sqlite3.connect(self.db.db_path)
        c = conn.cursor()
        c.execute(
            "SELECT video_id FROM videos WHERE user_id=? AND status=?",
            (self.user_id, status_val),
        )
        rows = c.fetchall()
        conn.close()
        return [r[0] for r in rows]

    #########################################################
    #               Secuid & Username Logic                 #
    #########################################################

    def _extract_secuid_once(self, tiktok_url: str, url_type: str) -> Optional[str]:
        """Use Playwright to parse the __UNIVERSAL_DATA_FOR_REHYDRATION__ for secUid."""
        if url_type == "user":
            secuid_traverse_list = [
                "__DEFAULT_SCOPE__",
                "webapp.user-detail",
                "userInfo",
                "user",
                "secUid",
            ]
            status_code_traverse_list = [
                "__DEFAULT_SCOPE__",
                "webapp.user-detail",
                "statusCode",
            ]
        elif url_type == "video":
            secuid_traverse_list = [
                "__DEFAULT_SCOPE__",
                "webapp.video-detail",
                "itemInfo",
                "itemStruct",
                "author",
                "secUid",
            ]
            status_code_traverse_list = [
                "__DEFAULT_SCOPE__",
                "webapp.video-detail",
                "statusCode",
            ]
        else:
            raise ValueError('url_type must be "user" or "video"')

        for retry in itertools.count():
            try:
                with sync_playwright() as p:
                    browser = p.firefox.launch(args=["--mute-audio"])
                    context = browser.new_context()
                    page = context.new_page()
                    page.goto(tiktok_url, wait_until="load")
                    time.sleep(2)

                    universal_data_json = page.evaluate(
                        """
                        () => {
                            const scriptElement = document.getElementById('__UNIVERSAL_DATA_FOR_REHYDRATION__');
                            if (scriptElement && scriptElement.type === 'application/json') {
                                return JSON.parse(scriptElement.textContent);
                            }
                            return null;
                        }
                        """
                    )
                    context.close()
                    browser.close()

                if not universal_data_json:
                    if retry < self.max_retries:
                        self._ifprinter(
                            f"Retrying playwright (no universal data): {retry + 1}/{self.max_retries}"
                        )
                        continue
                    else:
                        return None

                # Check status code
                status_code = self._traverse_dict(
                    universal_data_json, status_code_traverse_list
                )
                if status_code is None:
                    if retry < self.max_retries:
                        self._ifprinter(
                            f"Retrying playwright (no status code): {retry + 1}/{self.max_retries}"
                        )
                        continue
                    else:
                        return None

                if status_code != 0:
                    self._ifprinter(f"Error code {status_code} for {tiktok_url}")
                    return None

                # If status_code == 0, fetch secUid
                secuid = self._traverse_dict(universal_data_json, secuid_traverse_list)
                if not secuid:
                    if retry < self.max_retries:
                        self._ifprinter(
                            f"Retrying playwright (no secuid): {retry + 1}/{self.max_retries}"
                        )
                        continue
                    else:
                        return None
                else:
                    self._ifprinter("Successfully extracted Secondary User ID.")
                    return secuid

            except PlaywrightError as error:
                if retry < self.max_retries:
                    self._ifprinter(
                        f"Retrying playwright: {retry + 1}/{self.max_retries}"
                    )
                    continue
                else:
                    self._ifprinter(f"Max playwright retries exceeded. {error}")
                    return None

        return None

    def _get_user_name_from_secuid(self) -> Optional[str]:
        """
        Looks up the user name from the secuid we have, using the TikWM API.
        """
        if not self.secuid:
            return None

        params = {"unique_id": self.secuid, "count": "1", "cursor": "0"}
        if self.tikwm_api_sleep_time > 0:
            time.sleep(self.tikwm_api_sleep_time)

        try:
            r = requests.get(self.tikwm_api_user_posts_url, params=params)
            r.raise_for_status()
        except requests.RequestException:
            self._ifprinter("Failed to fetch user_name from secuid via TikWM.")
            return None

        data_json = r.json().get("data")
        if not data_json:
            return None

        video_list = data_json.get("videos", [])
        if not video_list:
            return None

        first_video = video_list[0]
        user_name = first_video.get("author", {}).get("unique_id")
        return user_name

    #########################################################
    #                 Video ID Extraction                   #
    #########################################################

    def _get_tiktok_links(self, user_name: str) -> None:
        """Construct main link and embed link for given user."""
        self.tiktok_user_link = f"https://www.tiktok.com/@{user_name}"
        self.tiktok_embed_link = f"https://www.tiktok.com/embed/@{user_name}"

    def _get_latest_video_id_list(self) -> Optional[List[str]]:
        """
        Returns the *latest* video IDs from the TikTok embed page or fallback if that fails.
        """
        try:
            embed_html = requests.get(self.tiktok_embed_link)
            embed_html.raise_for_status()
        except requests.RequestException:
            self._ifprinter(f"Failed to get embed HTML for {self.tiktok_embed_link}")
            return None

        match = re.search(
            r'(?s)<script[^>]+id=[\'"]__FRONTITY_CONNECT_STATE__[\'"][^>]*>([^<]+)</script>',
            embed_html.text,
        )
        if not match:
            self._ifprinter(
                f"No regex match in embed HTML for {self.tiktok_embed_link}"
            )
            return None

        try:
            data = json.loads(match.group(1))
        except json.JSONDecodeError:
            self._ifprinter("Failed to decode JSON from embed HTML.")
            return None

        embed_dict = self._traverse_dict(
            data, ["source", "data", f"/embed/@{self.user_name}"]
        )
        if not embed_dict:
            return None

        if "videoList" not in embed_dict:
            error_code = embed_dict.get("errorCode")
            if error_code:
                self._ifprinter(f"Embed error code {error_code}")
                return self._fallback_glvil()
            return None
        else:
            video_list = embed_dict["videoList"]
            if not video_list:
                return None
            out_ids = []
            for vid in video_list:
                vid_id = vid.get("id")
                if vid_id:
                    out_ids.append(vid_id)
            return out_ids if out_ids else None

    def _fallback_glvil(self) -> Optional[List[str]]:
        """
        Fallback method using Playwright to parse the user's page and
        extract any video links directly.
        """
        try:
            with sync_playwright() as p:
                browser = p.firefox.launch(args=["--mute-audio"])
                for retry in itertools.count():
                    context = browser.new_context()
                    try:
                        page = context.new_page()
                        page.goto(self.tiktok_user_link, wait_until="load")
                        time.sleep(2)

                        page_links = page.evaluate(
                            "() => Array.from(document.links).map(item => item.href)"
                        )
                        # Typically user’s video links are https://www.tiktok.com/@USER/video/<VID>
                        video_id_links = [
                            link.split("/")[-1]
                            for link in page_links
                            if link.startswith(self.tiktok_user_link + "/video/")
                        ]
                    except PlaywrightError as error:
                        context.close()
                        if retry < self.max_retries:
                            self._ifprinter(
                                f"Retrying playwright fallback: {retry + 1}/{self.max_retries}"
                            )
                            continue
                        else:
                            browser.close()
                            self._ifprinter(f"Max fallback retries exceeded: {error}")
                            return None
                    else:
                        context.close()
                        browser.close()
                        if video_id_links:
                            return video_id_links
                        else:
                            self._ifprinter("Found no fallback video IDs.")
                            return None
        except Exception as e:
            self._ifprinter(f"Failed fallback with exception: {str(e)}")
            return None

    def _get_video_id_list(self) -> List[str]:
        """
        Using the TikWM API to get ALL video IDs (with pagination).
        We'll break once we detect old content (complete or failed).
        """
        video_id_list = []
        cursor = "0"
        break_after = False

        for page in itertools.count(1):
            if self.tikwm_api_sleep_time > 0:
                time.sleep(self.tikwm_api_sleep_time)

            self._ifprinter(f"Page {page}", end="\r")
            params = {"unique_id": self.secuid, "count": "30", "cursor": cursor}

            try:
                r = requests.get(self.tikwm_api_user_posts_url, params=params)
                r.raise_for_status()
            except requests.RequestException:
                self._ifprinter("Failed to fetch user post data from TikWM.")
                break

            data_json = r.json().get("data", {})
            items = data_json.get("videos", [])

            for vid in items:
                video_id = vid.get("video_id")
                if not video_id:
                    continue

                if self._check_complete_or_failed(video_id):
                    break_after = True
                elif video_id not in video_id_list:
                    video_id_list.append(video_id)

            if break_after:
                break

            has_more = data_json.get("hasMore")
            if not has_more:
                break

            cursor = data_json.get("cursor", "0")
            if not cursor:
                break

        return video_id_list

    @staticmethod
    def _traverse_dict(object_: dict, keys: List[str]) -> Any:
        """Safely traverse nested dictionary keys."""
        current = object_
        for key in keys:
            if not isinstance(current, dict):
                return None
            current = current.get(key)
            if current is None:
                return None
        return current

    #########################################################
    #                 Download Orchestrator                 #
    #########################################################

    def _download_everything(
        self,
        video_id_list: List[str],
        print_number_of_files_saved: bool,
    ) -> None:
        """Download videos/images for given list of video IDs."""
        self.loop = asyncio.new_event_loop()
        results = self.loop.run_until_complete(
            self._download_all_video_ids(video_id_list)
        )

        self._ifprinter("Video download phase done. Checking images if needed.")
        for result in tqdm(results, leave=False):
            vid_id = result["video_id"]
            if result["is_image"]:
                # If HEAD check says "not a video," we try images
                self._download_images_from_video_id(vid_id)
            else:
                if result["success"]:
                    self.files_saved += 1
                    self.db.set_video_status(
                        vid_id,
                        self.user_id,
                        "complete",
                        str(self.user_folder.joinpath(f"{vid_id}.mp4")),
                    )
                else:
                    self.db.set_video_status(vid_id, self.user_id, "failed")

        self.loop.close()
        self.loop = None

        if print_number_of_files_saved:
            print(
                f"Downloaded {self.files_saved} new files for {self.original_user_name}"
            )

    async def _download_all_video_ids(
        self, video_id_list: List[str]
    ) -> List[Dict[str, Union[str, bool]]]:
        """Concurrent download for all video IDs in a list."""
        session = self._get_aiohttp_session(len(video_id_list))
        async with session:
            tasks = [
                self._download_single_video_id(vid, session) for vid in video_id_list
            ]
            results = await tqdm_asyncio.gather(*tasks, leave=False)
        return results

    async def _download_single_video_id(
        self, video_id: str, session: aiohttp.ClientSession
    ) -> Dict[str, Union[str, bool]]:
        """
        HEAD-check the TikWM video link. If it has 'mime_type=video_mp4', we treat as video.
        Otherwise, treat as images.
        """
        tikwm_video_url = self.tikwm_video_url_template.format(video_id=video_id)

        try:
            async with session.head(tikwm_video_url) as head_resp:
                headers = head_resp.headers
        except (aiohttp.ClientOSError, aiohttp.ClientPayloadError):
            return {"video_id": video_id, "is_image": True, "success": False}

        file_url = headers.get("location", "")
        if self.video_test_string in file_url:
            success = await self._download_video(video_id, file_url, session)
            return {"video_id": video_id, "is_image": False, "success": success}
        else:
            return {"video_id": video_id, "is_image": True, "success": False}

    #########################################################
    #            Video vs Image Download Helpers            #
    #########################################################

    async def _download_video(
        self,
        video_id: str,
        video_url: str,
        session: aiohttp.ClientSession,
    ) -> bool:
        """Download a single video file to disk."""
        filename = f"{video_id}.mp4"
        return await self._download_single_file(video_url, filename, video_id, session)

    def _download_images_from_video_id(self, video_id: str) -> None:
        """If not mp4, we assume it's an image set. Grab them and download."""
        image_list = self._extract_image_urls(video_id)
        if image_list is None:
            self.db.set_video_status(video_id, self.user_id, "failed")
            return

        if not self.loop:
            self.loop = asyncio.new_event_loop()

        results = self.loop.run_until_complete(
            self._download_images_from_image_url_list(video_id, image_list)
        )

        self.files_saved += sum(results)
        if all(results):
            self.db.set_video_status(video_id, self.user_id, "complete")
        else:
            self.db.set_video_status(video_id, self.user_id, "failed")

    async def _download_images_from_image_url_list(
        self, video_id: str, image_url_list: List[str]
    ) -> List[bool]:
        """Concurrent download of multiple images for a single video_id."""
        session = self._get_aiohttp_session(len(image_url_list))
        async with session:
            tasks = []
            for i, img_url in enumerate(image_url_list):
                tasks.append(self._download_image(video_id, i, img_url, session))
            results = await tqdm_asyncio.gather(*tasks, leave=False)
        return results

    def _extract_image_urls(self, video_id: str) -> Optional[List[str]]:
        """Call the TikWM /api with ?url=video_id, attempt to get 'images' list."""
        params = {"url": video_id}
        if self.tikwm_api_sleep_time > 0:
            time.sleep(self.tikwm_api_sleep_time)

        try:
            r = requests.get(self.tikwm_api_video_url, params=params)
            r.raise_for_status()
        except requests.RequestException:
            self._ifprinter(f"Failed to fetch image data for {video_id}")
            return None

        data_json = r.json().get("data", {})
        return data_json.get("images")

    async def _download_image(
        self,
        video_id: str,
        num: int,
        image_url: str,
        session: aiohttp.ClientSession,
    ) -> bool:
        """Download a single image file to disk."""
        filename = f"{video_id}_{num}.jpeg"
        return await self._download_single_file(image_url, filename, video_id, session)

    async def _download_single_file(
        self,
        media_url: str,
        filename: str,
        video_id: str,
        session: aiohttp.ClientSession,
    ) -> bool:
        """Download any single file (image/video) from a URL."""
        if self.save_with_date:
            filename, epoch_time = self._prepend_date(filename, video_id)
        else:
            epoch_time = None

        save_path = self.user_folder.joinpath(filename)

        file_exists = False
        if self.check_file_exists and save_path.is_file():
            file_exists = True

        write_file = True

        for retry in itertools.count():
            success = True
            try:
                async with session.get(media_url) as resp:
                    if resp.status == 200:
                        if file_exists:
                            remote_len_str = resp.headers.get("content-length")
                            if remote_len_str is None:
                                write_file = False
                            else:
                                remote_len = int(remote_len_str)
                                local_len = os.stat(save_path).st_size
                                if remote_len <= local_len:
                                    write_file = False

                        if write_file:
                            content = await resp.read()
                            async with aiofiles.open(save_path, "wb") as f:
                                await f.write(content)

                        if epoch_time is not None:
                            os.utime(save_path, (epoch_time, epoch_time))
                    else:
                        success = False
            except (aiohttp.ClientOSError, aiohttp.ClientPayloadError):
                success = False

            if success:
                return True
            else:
                if retry < self.max_retries:
                    if self.asyncio_error_sleep_time > 0:
                        await asyncio.sleep(self.asyncio_error_sleep_time)
                    continue
                else:
                    return False
        return False

    def _prepend_date(self, filename: str, video_id: str) -> Tuple[str, int]:
        """
        Prepend the epoch-based date from the TikTok video_id to the file name.
        (Approx logic for 64-bit Snowflake-like IDs.)
        """
        binary_str = bin(int(video_id))
        epoch_time = int(binary_str[:33], 2)
        date_obj = datetime.fromtimestamp(epoch_time, tz=timezone.utc)
        date_str = date_obj.strftime(self.date_format)
        new_filename = f"{date_str}_{filename}"
        return new_filename, epoch_time

    def _get_aiohttp_session(self, num_items: int) -> aiohttp.ClientSession:
        """
        Creates an aiohttp session with some custom timeouts.
        """
        connector = aiohttp.TCPConnector(limit=self.max_connections, force_close=True)
        total_timeout = max(300, self.timeout_per_file * num_items)
        timeout = aiohttp.ClientTimeout(total=total_timeout)
        return aiohttp.ClientSession(connector=connector, timeout=timeout)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "TikTok Scraper with SQLite-based metadata. "
            "Downloads go to script_dir/downloads; DB file is script_dir/ttdl.db"
        )
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    add_parser = subparsers.add_parser("add", help="Add user(s) to the DB")
    add_parser.add_argument("usernames", nargs="+", help="One or more usernames")

    run_parser = subparsers.add_parser("run", help="Scrape user(s)")
    run_parser.add_argument(
        "--user", "-u", type=str, help="Scrape a single user instead of all."
    )

    add_run_parser = subparsers.add_parser(
        "add-run", help="Add user(s), then run them immediately."
    )
    add_run_parser.add_argument("usernames", nargs="+", help="One or more usernames")

    return parser.parse_args()


def main():
    args = parse_args()
    script_dir = Path(__file__).parent
    db_path = script_dir / "ttdl.db"
    db = DatabaseManager(db_path)

    config = TikTokScraperConfig(
        max_retries=10,
        print_outputs=True,
        retry_failed=False,
    )
    scraper = TikTokScraper(config, db)

    if args.command == "add":
        for username in args.usernames:
            existing = db.get_user_by_name(username)
            if not existing:
                user_id = db.create_user(username)
                print(f"Added user '{username}' (id: {user_id}).")
            else:
                print(f"'{username}' is already in DB (id: {existing['user_id']}).")

    elif args.command == "run":
        if args.user:
            scraper.run_single_user(args.user)
        else:
            scraper.run_all_users()

    elif args.command == "add-run":
        for username in args.usernames:
            existing = db.get_user_by_name(username)
            if not existing:
                user_id = db.create_user(username)
                print(f"Added user '{username}' (id: {user_id}).")
            else:
                user_id = existing["user_id"]
                print(f"'{username}' is already in DB (id: {user_id}).")
            scraper.run_single_user(username)
    else:
        print("Unknown command.")


if __name__ == "__main__":
    main()
