import asyncio
import logging
import os
import tempfile
import threading
import zipfile
from collections import defaultdict
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath

import img2pdf
from natsort import natsorted
from PIL import Image
from pyrogram import Client, filters
from pyrogram.errors import FloodWait, MessageNotModified
from pyrogram.types import Message

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]

MAX_MB = int(os.environ.get("MAX_FILE_SIZE_MB", 125))
MAX_BYTES = MAX_MB * 1024 * 1024
SUPPORTED = {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tiff", ".tif", ".gif"}

logging.basicConfig(format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO)
log = logging.getLogger(__name__)


class ConversionCancelled(Exception):
    pass


@dataclass
class UserState:
    queue: asyncio.Queue[Message] = field(default_factory=asyncio.Queue)
    worker_task: asyncio.Task | None = None
    active_task: asyncio.Task | None = None
    cancel_event: asyncio.Event = field(default_factory=asyncio.Event)
    thread_cancel: threading.Event | None = None
    progress_message_id: int | None = None


user_states: dict[int, UserState] = defaultdict(UserState)


def _safe_archive_path(name: str) -> bool:
    pure = PurePosixPath(name)
    return not pure.is_absolute() and ".." not in pure.parts


def extract_cbz(cbz_path: Path, out_dir: Path, cancel_event: threading.Event) -> list[Path]:
    if cancel_event.is_set():
        raise ConversionCancelled
    if not zipfile.is_zipfile(cbz_path):
        raise ValueError("invalid cbz")
    with zipfile.ZipFile(cbz_path, "r") as zf:
        for member in zf.infolist():
            if cancel_event.is_set():
                raise ConversionCancelled
            if not _safe_archive_path(member.filename):
                raise ValueError("unsafe archive")
            zf.extract(member, out_dir)
    images = [p for p in out_dir.rglob("*") if p.is_file() and p.suffix.lower() in SUPPORTED]
    if not images:
        raise ValueError("no images")
    return natsorted(images, key=lambda p: str(p.relative_to(out_dir)).lower())


def convert_to_pdf(images: list[Path], pdf_path: Path, cancel_event: threading.Event) -> None:
    safe_images: list[Path] = []
    temp_images: list[Path] = []
    for image_path in images:
        if cancel_event.is_set():
            raise ConversionCancelled
        try:
            with Image.open(image_path) as image:
                if image.mode in ("RGBA", "LA", "P"):
                    converted = image_path.with_suffix(".converted.jpg")
                    image.convert("RGB").save(converted, format="JPEG", quality=95)
                    safe_images.append(converted)
                    temp_images.append(converted)
                else:
                    safe_images.append(image_path)
        except Exception:
            continue
    if not safe_images:
        raise ValueError("unreadable images")
    if cancel_event.is_set():
        raise ConversionCancelled
    try:
        with open(pdf_path, "wb") as output:
            output.write(img2pdf.convert([str(path) for path in safe_images]))
    except Exception:
        pil_images = []
        try:
            for path in safe_images:
                if cancel_event.is_set():
                    raise ConversionCancelled
                with Image.open(path) as image:
                    pil_images.append(image.convert("RGB").copy())
            if not pil_images:
                raise ValueError("unable to convert")
            pil_images[0].save(pdf_path, save_all=True, append_images=pil_images[1:], format="PDF")
        finally:
            for image in pil_images:
                with suppress(Exception):
                    image.close()
    finally:
        for path in temp_images:
            with suppress(Exception):
                path.unlink()


async def update_progress(client: Client, chat_id: int, state: UserState, step: str, percent: int) -> None:
    if state.cancel_event.is_set():
        raise asyncio.CancelledError
    text = f"{step}\n{percent}%"
    try:
        if state.progress_message_id is None:
            message = await client.send_message(chat_id, text)
            state.progress_message_id = message.id
        else:
            await client.edit_message_text(chat_id, state.progress_message_id, text)
    except FloodWait as wait_error:
        await asyncio.sleep(wait_error.value)
        if state.progress_message_id is None:
            message = await client.send_message(chat_id, text)
            state.progress_message_id = message.id
        else:
            with suppress(MessageNotModified):
                await client.edit_message_text(chat_id, state.progress_message_id, text)
    except MessageNotModified:
        pass


async def delete_progress(client: Client, chat_id: int, state: UserState) -> None:
    if state.progress_message_id is None:
        return
    with suppress(Exception):
        await client.delete_messages(chat_id, state.progress_message_id)
    state.progress_message_id = None


async def process_cbz_message(client: Client, chat_id: int, message: Message, state: UserState) -> None:
    document = message.document
    if document is None:
        return
    if (document.file_size or 0) > MAX_BYTES:
        return

    file_name = document.file_name or "file.cbz"
    pdf_name = f"{Path(file_name).stem}.pdf"

    state.thread_cancel = threading.Event()
    loop = asyncio.get_running_loop()
    try:
        with tempfile.TemporaryDirectory(prefix="cbzbot_") as tmpdir:
            work_dir = Path(tmpdir)
            cbz_path = work_dir / file_name
            extract_dir = work_dir / "extract"
            extract_dir.mkdir(parents=True, exist_ok=True)
            pdf_path = work_dir / pdf_name

            await update_progress(client, chat_id, state, "Downloading", 0)
            await message.download(file_name=str(cbz_path))
            await update_progress(client, chat_id, state, "Downloading", 25)

            await update_progress(client, chat_id, state, "Extracting", 40)
            images = await loop.run_in_executor(None, extract_cbz, cbz_path, extract_dir, state.thread_cancel)
            await update_progress(client, chat_id, state, "Extracting", 55)

            await update_progress(client, chat_id, state, "Converting", 70)
            await loop.run_in_executor(None, convert_to_pdf, images, pdf_path, state.thread_cancel)
            await update_progress(client, chat_id, state, "Converting", 90)

            await update_progress(client, chat_id, state, "Uploading", 95)
            await client.send_document(chat_id=chat_id, document=str(pdf_path), file_name=pdf_name)
            await update_progress(client, chat_id, state, "Uploading", 100)
            await delete_progress(client, chat_id, state)
    except (asyncio.CancelledError, ConversionCancelled):
        raise
    except Exception as exc:
        await delete_progress(client, chat_id, state)
        with suppress(Exception):
            await client.send_message(chat_id, f"❌ Conversion failed: {exc}")
    finally:
        state.thread_cancel = None


async def user_worker(client: Client, chat_id: int) -> None:
    state = user_states[chat_id]
    try:
        while True:
            try:
                message = await state.queue.get()
            except asyncio.CancelledError:
                break

            state.active_task = asyncio.create_task(process_cbz_message(client, chat_id, message, state))
            try:
                await state.active_task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
            finally:
                state.active_task = None
                state.queue.task_done()

            if state.queue.empty():
                break
    finally:
        state.worker_task = None
        state.cancel_event.clear()


async def clear_queue(state: UserState) -> None:
    while True:
        try:
            state.queue.get_nowait()
            state.queue.task_done()
        except asyncio.QueueEmpty:
            break


app = Client("cbz_to_pdf_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)


@app.on_message(filters.command("cancel"))
async def cancel_handler(client: Client, message: Message) -> None:
    if message.chat is None:
        return
    chat_id = message.chat.id
    state = user_states[chat_id]
    state.cancel_event.set()
    if state.thread_cancel is not None:
        state.thread_cancel.set()
    await clear_queue(state)
    if state.active_task and not state.active_task.done():
        state.active_task.cancel()
    if state.worker_task and not state.worker_task.done():
        state.worker_task.cancel()
    await delete_progress(client, chat_id, state)
    state.cancel_event.clear()


@app.on_message(filters.document)
async def document_handler(client: Client, message: Message) -> None:
    if message.chat is None or message.document is None:
        return
    file_name = (message.document.file_name or "").lower()
    if not file_name.endswith(".cbz"):
        return

    chat_id = message.chat.id
    state = user_states[chat_id]
    await state.queue.put(message)
    if state.worker_task is None or state.worker_task.done():
        state.worker_task = asyncio.create_task(user_worker(client, chat_id))


if __name__ == "__main__":
    app.run()
