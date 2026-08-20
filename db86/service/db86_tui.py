"""
DB86 Text-based User Interface (TUI)

A comprehensive terminal UI for managing DB86 REST service databases, storages, and items.
Built with Textual framework for rich terminal interactions.
"""

import json
import asyncio
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
from urllib import request, parse, error


from textual.app import ComposeResult, on
from textual.containers import Container, Horizontal, Vertical, VerticalScroll, Grid
from textual.widgets import (
    Header,
    Footer,
    Button,
    Label,
    Input,
    TextArea,
    Select,
    DataTable,
    Static,
    TabbedContent,
    TabPane,
    OptionList
)
from textual.widgets.option_list import Option
from textual.reactive import reactive
from textual.screen import Screen, ModalScreen
from textual.binding import Binding
from textual.app import App
from textual.message import Message


@dataclass
class ConnectionConfig:
    """Configuration for REST service connection."""
    host: str = "127.0.0.1"
    port: int = 8000

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"


class APIClient:
    """Async wrapper around urllib using asyncio.to_thread."""

    def __init__(self, config: ConnectionConfig):
        self.config = config

    # --- internal sync helpers (run in thread) ---
    def _build_url(self, path: str, params: Optional[Dict[str, Any]] = None) -> str:
        url = parse.urljoin(self.config.base_url + "/", path.lstrip("/"))
        if params:
            qs = parse.urlencode(params)
            url = f"{url}?{qs}"
        return url

    def _sync_request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Any] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        url = self._build_url(path, params)
        body = None
        hdrs = {"Accept": "application/json"}
        if headers:
            hdrs.update(headers)
        if json_body is not None:
            body = json.dumps(json_body).encode("utf-8")
            hdrs["Content-Type"] = "application/json"

        req = request.Request(url, data=body, headers=hdrs, method=method.upper())
        try:
            with request.urlopen(req, timeout=30) as resp:
                raw = resp.read()
                status = resp.getcode()
                # try to decode JSON, but fall back to empty dict/list
                try:
                    data = json.loads(raw.decode("utf-8")) if raw else {}
                except Exception:
                    data = {}
                return {"status": status, "data": data}
        except error.HTTPError as he:
            # HTTPError has code and may have body
            try:
                body = he.read()
                data = json.loads(body.decode("utf-8")) if body else {}
            except Exception:
                data = {}
            return {"status": he.code, "data": data}
        except Exception:
            return {"status": None, "data": {}}

    # --- async wrappers ---
    async def health_check(self) -> bool:
        try:
            res = await asyncio.to_thread(self._sync_request, "GET", "/")
            return res["status"] == 200
        except Exception:
            return False

    async def list_databases(self) -> List[str]:
        try:
            res = await asyncio.to_thread(self._sync_request, "GET", "/databases")
            return res["data"].get("databases", []) if isinstance(res["data"], dict) else []
        except Exception:
            return []

    async def get_database_metadata(self, db_name: str) -> Dict[str, Any]:
        try:
            res = await asyncio.to_thread(self._sync_request, "GET", f"/databases/{db_name}")
            return res["data"] if isinstance(res["data"], dict) else {}
        except Exception:
            return {}

    async def create_database(self, name: str, autocommit: bool = True, memory: bool = False) -> bool:
        try:
            payload = {"name": name, "autocommit": autocommit, "memory": memory}
            res = await asyncio.to_thread(self._sync_request, "POST", "/databases", json_body=payload)
            return res["status"] == 201
        except Exception:
            return False

    async def delete_database(self, db_name: str) -> bool:
        try:
            res = await asyncio.to_thread(self._sync_request, "DELETE", f"/databases/{db_name}")
            return res["status"] == 200
        except Exception:
            return False

    async def list_storages(self, db_name: str) -> List[Dict[str, str]]:
        try:
            res = await asyncio.to_thread(self._sync_request, "GET", f"/databases/{db_name}/storages")
            return res["data"].get("storages", []) if isinstance(res["data"], dict) else []
        except Exception:
            return []

    async def get_storage_metadata(self, db_name: str, storage_name: str) -> Dict[str, str]:
        try:
            res = await asyncio.to_thread(self._sync_request, "GET", f"/databases/{db_name}/storages/{storage_name}")
            return res["data"] if isinstance(res["data"], dict) else {}
        except Exception:
            return {}

    async def create_storage(self, db_name: str, storage_name: str, storage_type: str = "json") -> bool:
        try:
            payload = {"name": storage_name, "storage_type": storage_type}
            res = await asyncio.to_thread(
                self._sync_request, "POST", f"/databases/{db_name}/storages", json_body=payload
            )
            return res["status"] == 201
        except Exception:
            return False

    async def delete_storage(self, db_name: str, storage_name: str) -> bool:
        try:
            res = await asyncio.to_thread(
                self._sync_request, "DELETE", f"/databases/{db_name}/storages/{storage_name}"
            )
            return res["status"] == 200
        except Exception:
            return False

    async def list_items(self, db_name: str, storage_name: str, limit: int = 100, offset: int = 0) -> List[Dict]:
        try:
            params = {"limit": limit, "offset": offset}
            res = await asyncio.to_thread(
                self._sync_request,
                "GET",
                f"/databases/{db_name}/storages/{storage_name}/items",
                params=params,
            )
            return res["data"].get("items", []) if isinstance(res["data"], dict) else []
        except Exception:
            return []

    async def query_items(self, db_name: str, storage_name: str, recipe: Dict[str, Any]) -> List[Dict]:
        try:
            res = await asyncio.to_thread(
                self._sync_request,
                "POST",
                f"/databases/{db_name}/storages/{storage_name}/query",
                json_body=recipe,
            )
            return res["data"].get("items", []) if isinstance(res["data"], dict) else []
        except Exception:
            return []

    async def get_item(self, db_name: str, storage_name: str, item_key: str) -> Optional[Dict]:
        try:
            res = await asyncio.to_thread(
                self._sync_request,
                "GET",
                f"/databases/{db_name}/storages/{storage_name}/items/{item_key}",
            )
            return res["data"] if isinstance(res["data"], dict) else None
        except Exception:
            return None

    async def upsert_item(self, db_name: str, storage_name: str, item_key: str, value: Any) -> bool:
        try:
            payload = {"value": value}
            res = await asyncio.to_thread(
                self._sync_request,
                "PUT",
                f"/databases/{db_name}/storages/{storage_name}/items/{item_key}",
                json_body=payload,
            )
            return res["status"] in (200, 201)
        except Exception:
            return False

    async def delete_item(self, db_name: str, storage_name: str, item_key: str) -> bool:
        try:
            res = await asyncio.to_thread(
                self._sync_request,
                "DELETE",
                f"/databases/{db_name}/storages/{storage_name}/items/{item_key}",
            )
            return res["status"] == 200
        except Exception:
            return False

    async def bulk_upsert_items(self, db_name: str, storage_name: str, items: Dict[str, Any]) -> bool:
        try:
            payload = {"items": items}
            res = await asyncio.to_thread(
                self._sync_request,
                "POST",
                f"/databases/{db_name}/storages/{storage_name}/items",
                json_body=payload,
            )
            return res["status"] == 200
        except Exception:
            return False

    async def close(self):
        """No persistent connection to close when using urllib; provided for API parity."""
        return None


class ConnectionModal(ModalScreen):
    """Modal for configuring connection settings."""

    DEFAULT_CSS = """
    ConnectionModal {
        align: center middle;
    }

    #connection-container {
        width: 60;
        height: 15;
        border: solid $accent;
        background: $surface;
    }

    #connection-container > Label {
        dock: top;
        height: 1;
    }

    .input-group {
        height: auto;
        margin: 1;
    }

    .input-group > Label {
        width: 12;
        text-align: right;
    }
    """

    def __init__(self, config: ConnectionConfig):
        super().__init__()
        self.config = config

    def compose(self) -> ComposeResult:
        yield Container(
            Label("Connection Settings"),
            Horizontal(
                Label("Host:", classes="input-label"),
                Input(value=self.config.host, id="host-input"),
                classes="input-group",
            ),
            Horizontal(
                Label("Port:", classes="input-label"),
                Input(value=str(self.config.port), id="port-input"),
                classes="input-group",
            ),
            Horizontal(
                Button("Connect", variant="primary", id="connect-btn"),
                Button("Cancel", variant="default", id="cancel-btn"),
            ),
            id="connection-container",
        )

    @on(Button.Pressed, "#connect-btn")
    def on_connect(self) -> None:
        host_input = self.query_one("#host-input", Input)
        port_input = self.query_one("#port-input", Input)

        try:
            self.config.host = host_input.value
            self.config.port = int(port_input.value)
            self.dismiss(True)
        except ValueError:
            # Invalid port, don't dismiss
            pass

    @on(Button.Pressed, "#cancel-btn")
    def on_cancel(self) -> None:
        self.dismiss(False)


class NewDatabaseModal(ModalScreen):
    """Modal for creating a new database."""

    DEFAULT_CSS = """
    NewDatabaseModal {
        align: center middle;
    }

    #db-container {
        width: 60;
        height: 16;
        border: solid $accent;
        background: $surface;
    }

    .input-group {
        height: auto;
        margin: 1;
    }
    """

    def compose(self) -> ComposeResult:
        yield Container(
            Label("Create Database"),
            Horizontal(
                Label("Name:", classes="input-label"),
                Input(id="db-name-input"),
                classes="input-group",
            ),
            Horizontal(
                Label("Memory DB:", classes="input-label"),
                Select([("No", "false"), ("Yes", "true")], value="false", id="memory-select"),
                classes="input-group",
            ),
            Horizontal(
                Button("Create", variant="primary", id="create-db-btn"),
                Button("Cancel", variant="default", id="cancel-db-btn"),
            ),
            id="db-container",
        )

    @on(Button.Pressed, "#create-db-btn")
    def on_create(self) -> None:
        name_input = self.query_one("#db-name-input", Input)
        memory_select = self.query_one("#memory-select", Select)

        if name_input.value:
            self.dismiss({
                "name": name_input.value,
                "memory": memory_select.value == "true"
            })

    @on(Button.Pressed, "#cancel-db-btn")
    def on_cancel(self) -> None:
        self.dismiss(None)


class NewStorageModal(ModalScreen):
    """Modal for creating a new storage."""

    DEFAULT_CSS = """
    NewStorageModal {
        align: center middle;
    }

    #storage-container {
        width: 60;
        height: 16;
        border: solid $accent;
        background: $surface;
    }

    .input-group {
        height: auto;
        margin: 1;
    }
    """

    def compose(self) -> ComposeResult:
        yield Container(
            Label("Create Storage"),
            Horizontal(
                Label("Name:", classes="input-label"),
                Input(id="storage-name-input"),
                classes="input-group",
            ),
            Horizontal(
                Label("Type:", classes="input-label"),
                Select([("JSON", "json"), ("Table", "table")], value="json", id="storage-type-select"),
                classes="input-group",
            ),
            Horizontal(
                Button("Create", variant="primary", id="create-storage-btn"),
                Button("Cancel", variant="default", id="cancel-storage-btn"),
            ),
            id="storage-container",
        )

    @on(Button.Pressed, "#create-storage-btn")
    def on_create(self) -> None:
        name_input = self.query_one("#storage-name-input", Input)
        type_select = self.query_one("#storage-type-select", Select)

        if name_input.value:
            self.dismiss({
                "name": name_input.value,
                "storage_type": type_select.value
            })

    @on(Button.Pressed, "#cancel-storage-btn")
    def on_cancel(self) -> None:
        self.dismiss(None)


class ItemEditorModal(ModalScreen):
    """Modal for editing a single item."""

    DEFAULT_CSS = """
    ItemEditorModal {
        align: center middle;
    }

    #item-editor-container {
        width: 80;
        height: 30;
        border: solid $accent;
        background: $surface;
    }

    #item-key-input {
        width: 100%;
        height: 1;
    }

    #item-value-textarea {
        width: 100%;
        height: 20;
    }
    """

    def __init__(self, item_key: Optional[str] = None, item_value: Optional[Any] = None):
        super().__init__()
        self.item_key = item_key or ""
        self.item_value = item_value or {}

    def compose(self) -> ComposeResult:
        value_str = json.dumps(self.item_value, indent=2) if isinstance(self.item_value, (dict, list)) else str(self.item_value)

        yield Container(
            Label("Edit Item"),
            Label("Key:"),
            Input(value=self.item_key, id="item-key-input"),
            Label("Value (JSON):"),
            TextArea(text=value_str, id="item-value-textarea"),
            Horizontal(
                Button("Save", variant="primary", id="save-item-btn"),
                Button("Cancel", variant="default", id="cancel-item-btn"),
            ),
            id="item-editor-container",
        )

    @on(Button.Pressed, "#save-item-btn")
    def on_save(self) -> None:
        key_input = self.query_one("#item-key-input", Input)
        value_textarea = self.query_one("#item-value-textarea", TextArea)

        try:
            value = json.loads(value_textarea.text)
            self.dismiss({"key": key_input.value, "value": value})
        except json.JSONDecodeError:
            # Try to save as string if JSON parsing fails
            self.dismiss({"key": key_input.value, "value": value_textarea.text})

    @on(Button.Pressed, "#cancel-item-btn")
    def on_cancel(self) -> None:
        self.dismiss(None)


class DatabaseBrowser(Static):
    """Widget for browsing databases."""

    databases: reactive[List[str]] = reactive([])
    selected_database: reactive[Optional[str]] = reactive(None)

    DEFAULT_CSS = """
    DatabaseBrowser {
        width: 1fr;
        height: 1fr;
    }

    #databases-list {
        width: 1fr;
        height: 1fr;
    }

    #db-controls {
        width: 1fr;
        height: auto;
        dock: bottom;
        border-top: solid $accent;
    }
    """

    class DatabaseSelected(Message):
        """Posted when a database is selected."""
        def __init__(self, database_name: Optional[str]) -> None:
            super().__init__()
            self.database_name = database_name

    def __init__(self, api_client: APIClient):
        super().__init__()
        self.api_client = api_client

    def compose(self) -> ComposeResult:
        yield Label("Databases", id="db-title")
        yield OptionList(id="databases-list")
        yield Horizontal(
            Button("New", id="new-db-btn", variant="primary"),
            Button("Refresh", id="refresh-db-btn"),
            Button("Delete", id="delete-db-btn", variant="error"),
            id="db-controls",
        )

    async def refresh_databases(self) -> None:
        """Refresh the database list."""
        databases = await self.api_client.list_databases()
        self.databases = databases
        option_list = self.query_one("#databases-list", OptionList)
        option_list.clear_options()
        for db in databases:
            option_list.add_option(Option(db, id=db))

    @on(OptionList.OptionSelected, "#databases-list")
    def on_database_selected(self, event: OptionList.OptionSelected) -> None:
        self.selected_database = event.option.prompt if event.option else None
        self.post_message(self.DatabaseSelected(self.selected_database))

    @on(Button.Pressed, "#new-db-btn")
    async def on_new_database(self) -> None:
        def show_modal(result):
            if result:
                asyncio.create_task(self._create_database(result["name"], result["memory"]))

        self.app.push_screen(NewDatabaseModal(ConnectionConfig()), show_modal)

    @on(Button.Pressed, "#refresh-db-btn")
    async def on_refresh_databases(self) -> None:
        await self.refresh_databases()

    @on(Button.Pressed, "#delete-db-btn")
    async def on_delete_database(self) -> None:
        if self.selected_database:
            await self.api_client.delete_database(self.selected_database)
            await self.refresh_databases()

    async def _create_database(self, name: str, memory: bool) -> None:
        await self.api_client.create_database(name, memory=memory)
        await self.refresh_databases()


class StorageBrowser(Static):
    """Widget for browsing storages in a database."""

    storages: reactive[List[Dict[str, str]]] = reactive([])
    selected_storage: reactive[Optional[str]] = reactive(None)
    current_database: reactive[Optional[str]] = reactive(None)

    DEFAULT_CSS = """
    StorageBrowser {
        width: 1fr;
        height: 1fr;
    }

    #storages-list {
        width: 1fr;
        height: 1fr;
    }

    #storage-controls {
        width: 1fr;
        height: auto;
        dock: bottom;
        border-top: solid $accent;
    }
    """

    class StorageSelected(Message):
        """Posted when a storage is selected."""
        def __init__(self, database_name: Optional[str], storage_name: Optional[str]) -> None:
            super().__init__()
            self.database_name = database_name
            self.storage_name = storage_name

    def __init__(self, api_client: APIClient):
        super().__init__()
        self.api_client = api_client

    def compose(self) -> ComposeResult:
        yield Label("Storages", id="storage-title")
        yield OptionList(id="storages-list")
        yield Horizontal(
            Button("New", id="new-storage-btn", variant="primary"),
            Button("Refresh", id="refresh-storage-btn"),
            Button("Delete", id="delete-storage-btn", variant="error"),
            id="storage-controls",
        )

    def watch_current_database(self, new_db: Optional[str]) -> None:
        """Refresh storages when database changes."""
        if new_db:
            asyncio.create_task(self.refresh_storages(new_db))

    async def refresh_storages(self, db_name: str) -> None:
        """Refresh the storage list for a database."""
        storages = await self.api_client.list_storages(db_name)
        self.storages = storages
        option_list = self.query_one("#storages-list", OptionList)
        option_list.clear_options()
        for storage in storages:
            label = f"{storage['name']} ({storage['storage_type']})"
            option_list.add_option(Option(label, id=storage['name']))

    @on(OptionList.OptionSelected, "#storages-list")
    def on_storage_selected(self, event: OptionList.OptionSelected) -> None:
        if event.option:
            self.selected_storage = event.option.id
            self.post_message(self.StorageSelected(self.current_database, self.selected_storage))

    @on(Button.Pressed, "#new-storage-btn")
    async def on_new_storage(self) -> None:
        if not self.current_database:
            return

        def show_modal(result):
            if result and self.current_database:
                asyncio.create_task(self._create_storage(self.current_database, result["name"], result["storage_type"]))

        self.app.push_screen(NewStorageModal(), show_modal)

    @on(Button.Pressed, "#refresh-storage-btn")
    async def on_refresh_storages(self) -> None:
        if self.current_database:
            await self.refresh_storages(self.current_database)

    @on(Button.Pressed, "#delete-storage-btn")
    async def on_delete_storage(self) -> None:
        if self.current_database and self.selected_storage:
            await self.api_client.delete_storage(self.current_database, self.selected_storage)
            await self.refresh_storages(self.current_database)

    async def _create_storage(self, db_name: str, name: str, storage_type: str) -> None:
        await self.api_client.create_storage(db_name, name, storage_type)
        await self.refresh_storages(db_name)


class ItemBrowser(Static):
    """Widget for browsing items in a storage."""

    items: reactive[List[Dict]] = reactive([])
    current_database: reactive[Optional[str]] = reactive(None)
    current_storage: reactive[Optional[str]] = reactive(None)
    query: reactive[Dict] = reactive({"offset": 0, "limit": 100})
    page_offset = 0
    page_size = 100

    DEFAULT_CSS = """
    ItemBrowser {
        width: 1fr;
        height: 1fr;
    }

    #items-table {
        width: 1fr;
        height: 1fr;
    }

    #item-controls {
        width: 1fr;
        height: auto;
        dock: bottom;
        border-top: solid $accent;
    }

    #item-controls Button {
        width: 10;
        min-width: 5;
        margin: 0 0;
    }

    #item-pagination {
        width: 1fr;
        height: auto;
    }
    """

    def __init__(self, api_client: APIClient):
        super().__init__()
        self.api_client = api_client

    def compose(self) -> ComposeResult:
        yield Label("Items", id="item-title")
        yield DataTable(id="items-table")
        yield Horizontal(
            Button("New", id="new-item-btn", variant="primary"),
            Button("Edit", id="edit-item-btn"),
            Button("Refresh", id="refresh-item-btn"),
            Button("Delete", id="delete-item-btn", variant="error"),
            Button("Previous", id="previous-item-btn"),
            Button("Next", id="next-item-btn"),
            Button("Filter", id="filter-item-btn", variant="warning"),
            id="item-controls",
        )

    def on_mount(self) -> None:
        table = self.query_one("#items-table", DataTable)
        table.add_columns("Key", "Value")

    def watch_current_storage(self, _: Optional[str]) -> None:
        """Refresh items when storage changes."""
        self.page_offset = 0
        if self.current_database and self.current_storage:
            asyncio.create_task(self.refresh_items())

    async def refresh_items(self) -> None:
        """Refresh the items list."""
        if not self.current_database or not self.current_storage:
            return

        meta = await self.api_client.get_storage_metadata(self.current_database, self.current_storage)
        cols = self.query.get("select", meta.get("columns", ["Key", "Value"]) if isinstance(meta, dict) else ["Key", "Value"])
        entries = meta.get("entries", 0) if isinstance(meta, dict) else 0
        
        items = await self.api_client.query_items(self.current_database, self.current_storage, self.query)
        has_next = self.page_size + self.page_offset < entries
        self.items = items

        table = self.query_one("#items-table", DataTable)
        table.clear(columns=True)
        table.add_columns(*cols)

        for item in items:
            row = [str(item.get(col, ""))[:50] for col in cols]
            table.add_row(*row)
        self.query_one("#previous-item-btn", Button).disabled = self.page_offset == 0
        self.query_one("#next-item-btn", Button).disabled = not has_next

    @on(Button.Pressed, "#previous-item-btn")
    async def on_previous_items(self) -> None:
        self.page_size = self.query['limit']
        if self.page_offset >= self.page_size:
            self.page_offset -= self.page_size
        self.query['offset'] = self.page_offset
        await self.refresh_items()

    @on(Button.Pressed, "#next-item-btn")
    async def on_next_items(self) -> None:
        self.page_size = self.query['limit']
        self.page_offset += self.page_size
        self.query['offset'] = self.page_offset
        await self.refresh_items()

    @on(Button.Pressed, "#new-item-btn")
    async def on_new_item(self) -> None:
        def show_modal(result):
            if result and self.current_database and self.current_storage:
                asyncio.create_task(self._upsert_item(result["key"], result["value"]))

        self.app.push_screen(ItemEditorModal(), show_modal)

    @on(Button.Pressed, "#edit-item-btn")
    async def on_edit_item(self) -> None:
        table = self.query_one("#items-table", DataTable)
        if table.cursor_row is not None and table.cursor_row < len(self.items):
            item = self.items[table.cursor_row]

            def show_modal(result):
                if result and self.current_database and self.current_storage:
                    asyncio.create_task(self._upsert_item(result["key"], result["value"]))

            self.app.push_screen(
                ItemEditorModal(item.get("key", ""), item.get("value", {})),
                show_modal
            )

    @on(Button.Pressed, "#refresh-item-btn")
    async def on_refresh_items(self) -> None:
        self.page_offset = 0
        await self.refresh_items()

    @on(Button.Pressed, "#delete-item-btn")
    async def on_delete_item(self) -> None:
        table = self.query_one("#items-table", DataTable)
        if table.cursor_row is not None and table.cursor_row < len(self.items):
            item = self.items[table.cursor_row]
            if self.current_database and self.current_storage:
                await self.api_client.delete_item(
                    self.current_database,
                    self.current_storage,
                    item.get("key", "")
                )
                await self.refresh_items()

    @on(Button.Pressed, "#filter-item-btn")
    async def on_filter_items(self) -> None:
        def show_modal(result):
            if result and self.current_database and self.current_storage:
                self.query = dict(result)
                asyncio.create_task(self.refresh_items())

        self.app.push_screen(FilterModal(json.dumps(self.query, indent=4)), show_modal)

    async def _upsert_item(self, key: str, value: Any) -> None:
        if self.current_database and self.current_storage:
            await self.api_client.upsert_item(self.current_database, self.current_storage, key, value)
            await self.refresh_items()


class FilterModal(ModalScreen):
    """Popup modal for JSON filter input."""
    DEFAULT_CSS = """
    FilterModal {
        align: center middle;
    }

    #filter-container {
        width: 60;
        height: 20;
        border: solid $accent;
        background: $surface;
    }

    #filter-label {
        dock: top;
    }

    #filter-json-input {
        width: 1fr;
        height: 1fr;
        align: center middle;
    }

    #filter-buttons {
        dock: bottom;
        height: auto;
        align-horizontal: center;
        padding: 1 1;
    }

    #apply-filter-btn, #cancel-filter-btn {
        width: 20;
        min-width: 10;
        margin: 0 1;
    }
    """

    def __init__(self, initial_filter: str = "{}"):
        super().__init__()
        self.initial_filter = initial_filter

    def compose(self):
        yield Container(
            Label("Enter JSON filter:", id="filter-label"),
            TextArea(
                id="filter-json-input",
                text=self.initial_filter,
                language="json",
                tab_behavior="indent"
            ),
            Horizontal(
                Button("Apply Filter", id="apply-filter-btn", variant="primary"),
                Button("Cancel", id="cancel-filter-btn"),
                id="filter-buttons"
            ),
            id="filter-container"
        )

    @on(Button.Pressed, "#apply-filter-btn")
    def apply_filter(self):
        editor = self.query_one("#filter-json-input", TextArea)
        try:
            parsed = json.loads(editor.text)
            self.dismiss(parsed)   # return parsed JSON to caller
        except Exception as e:
            # You could show an error label here
            pass

    @on(Button.Pressed, "#cancel-filter-btn")
    def cancel_filter(self):
        self.dismiss(json.loads(self.initial_filter))


class StatusFooter(Horizontal):
    """Footer with server connection status indicator."""

    # Reactive state for online/offline
    is_online: reactive[bool] = reactive(False)

    DEFAULT_CSS = """
    StatusFooter {
        dock: bottom;
        height: 1;
    }
    StatusFooter > Footer {
        dock: none;      /* stop Footer from docking inside us */
        width: 1fr;
        height: 1;
    }
    StatusFooter > .status-bar {
        dock: none;
        margin-left: 1;
        width: auto;
        height: 1;
        padding: 0 1;
    }
    #status-indicator.online {
        color: green;
    }
    #status-indicator.offline {
        color: red;
    }
    """

    def compose(self):
        yield Footer()
        yield Horizontal(
            Label("o", id="status-indicator"),
            Label("Checking...", id="status-text"),
            classes="status-bar",
        )

    async def on_mount(self) -> None:
        self._poll_connection()
        self.set_interval(5, self._poll_connection)

    async def _poll_connection(self) -> None:
        import time
        if self.app.api_client:
            start = time.perf_counter()
            online = await self.app.api_client.health_check()
            latency = (time.perf_counter() - start) * 1000
            self.update_status(online, latency)

    def update_status(self, online: bool, latency_ms: float) -> None:
        """Update footer when status changes."""
        indicator = self.query_one("#status-indicator", Label)
        status_text = self.query_one("#status-text", Label)

        indicator.remove_class("online", "offline")
        indicator.add_class("online" if online else "offline")
        indicator.update("o")

        if online:
            if latency_ms is not None:
                status_text.update(f"Connected ({latency_ms:.1f} ms)")
            else:
                status_text.update("Connected")
        else:
            status_text.update("Disconnected")


class DB86TUI(App):
    """Main DB86 TUI Application."""

    CSS = """
    Screen {
        layout: grid;
        grid-size: 3 1;
        grid-columns: 1fr 1fr 2fr;
    }

    #left-panel {
        width: 1fr;
        border-right: solid $accent;
    }

    #middle-panel {
        width: 1fr;
        border-right: solid $accent;
    }

    #right-panel {
        width: 2fr;
    }
    """

    BINDINGS = [
        Binding("ctrl+c", "quit", "Quit", show=True),
        Binding("ctrl+s", "settings", "Settings", show=True),
    ]

    TITLE = "DB86 Terminal UI"

    def __init__(self):
        super().__init__()
        self.config = ConnectionConfig()
        self.api_client: Optional[APIClient] = None

    def compose(self) -> ComposeResult:
        yield Header()

        db_browser = DatabaseBrowser(self.api_client or APIClient(self.config))
        storage_browser = StorageBrowser(self.api_client or APIClient(self.config))
        item_browser = ItemBrowser(self.api_client or APIClient(self.config))

        self.db_browser = db_browser
        self.storage_browser = storage_browser
        self.item_browser = item_browser

        yield Container(
            db_browser,
            id="left-panel",
        )
        yield Container(
            storage_browser,
            id="middle-panel",
        )
        yield Container(
            item_browser,
            id="right-panel",
        )

        yield StatusFooter(id="status-footer")

    async def on_mount(self) -> None:
        """Initialize on mount."""
        self.api_client = APIClient(self.config)
        self.db_browser.api_client = self.api_client
        self.storage_browser.api_client = self.api_client
        self.item_browser.api_client = self.api_client

        await self._check_connection()


    async def _check_connection(self) -> None:
        """Check if we can connect to the service."""
        if await self.api_client.health_check():
            await self.db_browser.refresh_databases()

    def on_database_browser_database_selected(self, message: DatabaseBrowser.DatabaseSelected) -> None:
        """Handle database selection."""
        self.storage_browser.current_database = message.database_name
        self.item_browser.current_database = message.database_name

    def on_storage_browser_storage_selected(self, message: StorageBrowser.StorageSelected) -> None:
        """Handle storage selection."""
        self.item_browser.current_storage = message.storage_name
        self.item_browser.current_database = message.database_name

    def action_settings(self) -> None:
        """Show connection settings modal."""
        def on_result(result):
            if result:
                asyncio.create_task(self._reconnect())

        self.push_screen(ConnectionModal(self.config), on_result)

    async def _reconnect(self) -> None:
        """Reconnect with new settings."""
        if self.api_client:
            await self.api_client.close()

        self.api_client = APIClient(self.config)
        self.db_browser.api_client = self.api_client
        self.storage_browser.api_client = self.api_client
        self.item_browser.api_client = self.api_client

        await self._check_connection()


def run() -> None:
    """Run the TUI application."""
    app = DB86TUI()
    app.run()


if __name__ == "__main__":
    run()
