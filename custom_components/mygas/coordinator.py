"""MyGas Account Coordinator."""

from __future__ import annotations

from datetime import date
import logging
from random import randrange
from typing import Any

from aiomygas import MyGasApi, SimpleMyGasAuth
from aiomygas.exceptions import MyGasAuthError

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_PASSWORD, CONF_USERNAME
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryAuthFailed
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.debounce import Debouncer
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed
from homeassistant.util import dt as dt_util

from .const import (
    ATTR_ACCOUNT_ID,
    ATTR_ALIAS,
    ATTR_BALANCE,
    ATTR_COUNTERS,
    ATTR_ELS,
    ATTR_IS_ELS,
    ATTR_JNT_ACCOUNT_NUM,
    ATTR_LAST_UPDATE_TIME,
    ATTR_LSPU_INFO_GROUP,
    ATTR_UUID,
    CONF_ACCOUNT,
    CONF_ACCOUNTS,
    CONF_AUTO_UPDATE,
    CONF_INFO,
    DOMAIN,
    REQUEST_REFRESH_DEFAULT_COOLDOWN,
    UPDATE_HOUR_BEGIN,
    UPDATE_HOUR_END,
)
from .decorators import async_api_request_handler
from .helpers import get_update_interval, make_device_id


class MyGasCoordinator(DataUpdateCoordinator):
    """Coordinator is responsible for querying the device at a specified route."""

    _api: MyGasApi
    data: dict[str, Any]
    username: str
    password: str
    force_next_update: bool
    auto_update: bool

    def __init__(
        self,
        hass: HomeAssistant,
        logger: logging.Logger,
        *,
        config_entry: ConfigEntry,
    ) -> None:
        """Initialise a custom coordinator."""
        super().__init__(
            hass,
            logger,
            name=DOMAIN,
            request_refresh_debouncer=Debouncer(
                hass,
                logger,
                cooldown=REQUEST_REFRESH_DEFAULT_COOLDOWN,
                immediate=False,
            ),
        )
        self.force_next_update = False
        self.data = {}
        session = async_get_clientsession(hass)
        self.username = config_entry.data[CONF_USERNAME]
        self.password = config_entry.data[CONF_PASSWORD]
        self.auto_update = config_entry.data.get(CONF_AUTO_UPDATE, False)
        auth = SimpleMyGasAuth(self.username, self.password, session)
        self._api = MyGasApi(auth)

    async def async_force_refresh(self) -> None:
        """Force refresh data."""
        self.force_next_update = True
        await self.async_refresh()

    @staticmethod
    def _get_lspu_list(accounts_info: Any) -> list[dict[str, Any]]:
        """Return LSPU list from accounts_info with fallback keys."""
        if not isinstance(accounts_info, dict):
            return []
        lspu_list = (
            accounts_info.get("lspu")
            or accounts_info.get("lspuGroup")
            or accounts_info.get("lspuInfoGroup")
            or []
        )
        return lspu_list if isinstance(lspu_list, list) else []

    def _extract_balance_from_info(self, info: Any) -> float | None:
        """Extract balance from lspuInfo/elsInfo structures returned by API.

        Preferred (as in user's script):
          lspuInfo.info.services[0].balance
        Fallback:
          lspuInfo.info.balance
        """
        try:
            if not isinstance(info, dict):
                return None

            for _key, items in info.items():
                if not items:
                    continue

                # Usually list[dict] for LSPU
                if isinstance(items, list):
                    for item in items:
                        if not isinstance(item, dict):
                            continue

                        # 1) services[0].balance
                        services = item.get("services")
                        if isinstance(services, list) and services:
                            s0 = services[0]
                            if isinstance(s0, dict) and s0.get("balance") is not None:
                                return float(s0["balance"])

                        # 2) info.balance
                        if item.get("balance") is not None:
                            return float(item["balance"])

                # Sometimes dict
                if isinstance(items, dict):
                    services = items.get("services")
                    if isinstance(services, list) and services:
                        s0 = services[0]
                        if isinstance(s0, dict) and s0.get("balance") is not None:
                            return float(s0["balance"])
                    if items.get("balance") is not None:
                        return float(items["balance"])

        except (TypeError, ValueError):
            return None

        return None

    async def _async_update_data(self) -> dict[str, Any] | None:
        """Fetch data from MyGas."""
        _data: dict[str, Any] = self.data if self.data is not None else {}
        new_data: dict[str, Any] = {
            ATTR_LAST_UPDATE_TIME: dt_util.now(),
        }
        self.logger.debug("Start updating data...")

        try:
            accounts_info = _data.get(CONF_ACCOUNTS)

            if accounts_info is None or self.force_next_update:
                # get account general information
                self.logger.debug("Get accounts info for %s", self.username)
                accounts_info = await self._async_get_accounts()
                if accounts_info:
                    self.logger.debug(
                        "Accounts info for %s retrieved successfully", self.username
                    )
                else:
                    self.logger.warning(
                        "Accounts info for %s not retrieved", self.username
                    )
                    return new_data
            else:
                self.logger.debug(
                    "Accounts info for %s retrieved from cache", self.username
                )

            new_data[CONF_ACCOUNTS] = accounts_info

            # --- Robust branch detection + logging ---
            els_group = (
                accounts_info.get("elsGroup") if isinstance(accounts_info, dict) else None
            )
            lspu_list = self._get_lspu_list(accounts_info)

            self.logger.debug(
                "MYGAS accounts_info keys=%s | elsGroup=%s | lspu_list_len=%s",
                list(accounts_info.keys()) if isinstance(accounts_info, dict) else type(accounts_info),
                bool(els_group),
                len(lspu_list),
            )

            # --- Fill CONF_INFO and balance ---
            if els_group:
                self.logger.debug(
                    "Accounts info for els accounts %s retrieved successfully",
                    self.username,
                )
                new_data[ATTR_IS_ELS] = True
                new_data[CONF_INFO] = await self.retrieve_els_accounts_info(accounts_info)

                balance = self._extract_balance_from_info(new_data.get(CONF_INFO))
                if balance is not None:
                    new_data[ATTR_BALANCE] = abs(float(balance))

            elif lspu_list:
                self.logger.debug(
                    "Accounts info for lspu accounts %s retrieved successfully",
                    self.username,
                )
                new_data[ATTR_IS_ELS] = False
                new_data[CONF_INFO] = await self.retrieve_lspu_accounts_info(accounts_info)

                balance = self._extract_balance_from_info(new_data.get(CONF_INFO))
                if balance is not None:
                    new_data[ATTR_BALANCE] = abs(float(balance))

            else:
                self.logger.warning(
                    "Account %s: no elsGroup and no lspu list in accounts_info",
                    self.username,
                )
                # Important: don't return None; keep integration alive
                new_data[CONF_INFO] = {}
                new_data[ATTR_IS_ELS] = False
                return new_data

        except MyGasAuthError as exc:
            raise ConfigEntryAuthFailed("Incorrect Login or Password") from exc
        except Exception as exc:  # pylint: disable=broad-except
            raise UpdateFailed(f"Error communicating with API: {exc}") from exc
        else:
            self.logger.debug("Data updated successfully for %s", self.username)
            self.logger.debug("MYGAS new_data keys (final): %s", list(new_data.keys()))
            self.logger.debug("%s", new_data)
            return new_data
        finally:
            self.force_next_update = False
            if self.auto_update:
                self.update_interval = get_update_interval(
                    randrange(UPDATE_HOUR_BEGIN, UPDATE_HOUR_END),
                    randrange(60),
                    randrange(60),
                )
                self.logger.debug(
                    "Update interval: %s seconds", self.update_interval.total_seconds()
                )
            else:
                self.update_interval = None

    async def retrieve_els_accounts_info(self, accounts_info: dict[str, Any]) -> dict[int, Any]:
        """Retrieve ELS accounts info."""
        els_list = accounts_info.get("elsGroup") or []
        els_info: dict[int, Any] = {}
        for els in els_list:
            els_id = els.get("els", {}).get("id")
            if not els_id:
                self.logger.warning("id not found in els info")
                continue
            els_id = int(els_id)
            self.logger.debug("Get els info for %d", els_id)
            els_item_info = await self._async_get_els_info(els_id)
            if els_item_info:
                els_info[els_id] = els_item_info
                self.logger.debug("Els info for id=%d retrieved successfully", els_id)
            else:
                self.logger.warning("Els info for id=%d not retrieved", els_id)
        return els_info

    async def retrieve_lspu_accounts_info(
        self, accounts_info: dict[str, Any]
    ) -> dict[int, list[dict[str, Any]]]:
        """Retrieve LSPU accounts info."""
        lspu_list = self._get_lspu_list(accounts_info)
        lspu_info: dict[int, list[dict[str, Any]]] = {}

        for lspu in lspu_list:
            lspu_id = lspu.get("id")
            if not lspu_id:
                self.logger.warning("id not found in lspu info item: %s", lspu)
                continue

            lspu_id = int(lspu_id)
            self.logger.debug("Get lspu info for %s", lspu_id)

            lspu_item_info = await self._async_get_lspu_info(lspu_id)
            if lspu_item_info:
                if isinstance(lspu_item_info, list):
                    lspu_info[lspu_id] = lspu_item_info
                else:
                    lspu_info[lspu_id] = [lspu_item_info]
                self.logger.debug("Lspu info for %s retrieved successfully", lspu_id)
            else:
                self.logger.warning("Lspu info for %s not retrieved", lspu_id)

        return lspu_info

    def get_accounts(self) -> dict[int, dict[str | int, Any]]:
        """Get accounts info."""
        return self.data.get(CONF_INFO, {})

    def get_account_number(self, account_id: int, lspu_account_id: int) -> str:
        """Get account number."""
        account = self.get_accounts()[account_id]
        if self.is_els():
            _account_number = account.get(ATTR_ELS, {}).get(ATTR_JNT_ACCOUNT_NUM)
        else:
            _account_number = account[lspu_account_id].get(CONF_ACCOUNT)
        return _account_number

    def get_account_alias(self, account_id: int, lspu_account_id: int) -> str | None:
        """Get account alias."""
        account = self.get_accounts()[account_id]
        if self.is_els():
            _account_alias = account.get(ATTR_ELS, {}).get(ATTR_ALIAS)
        else:
            _account_alias = account[lspu_account_id].get(ATTR_ALIAS)
        return _account_alias

    def is_els(self) -> bool:
        """Account is ELS."""
        return self.data.get(ATTR_IS_ELS, False)

    def get_lspu_accounts(self, account_id: int) -> list[dict[str | int, Any]]:
        """Get LSPU accounts."""
        _data = self.get_accounts()[account_id]
        if self.is_els():
            _lspu_accounts = _data[ATTR_LSPU_INFO_GROUP]
        else:
            _lspu_accounts = _data if isinstance(_data, list) else [_data]
        return _lspu_accounts

    def get_counters(
        self, account_id: int, lspu_acount_id: int
    ) -> list[dict[str, Any]]:
        """Get counter data."""
        _accounts = self.get_lspu_accounts(account_id)[lspu_acount_id]
        counters = _accounts.get(ATTR_COUNTERS, [])
        if not counters:
            # This is a normal situation (account may not have counters)
            self.logger.debug(
                "No counters found for account_id=%d lspu_account_id=%d",
                account_id,
                lspu_acount_id,
            )
        return counters

    async def find_account_by_device_id(
        self, device_id: str
    ) -> tuple[int | None, int | None, int | None] | None:
        """Find device by id."""
        device_registry = dr.async_get(self.hass)
        device = device_registry.async_get(device_id)
        assert device

        for account_id in self.get_accounts():
            for lspu_account_id in range(len(self.get_lspu_accounts(account_id))):
                for counter_id in range(
                    len(self.get_counters(account_id, lspu_account_id))
                ):
                    _account_number = self.get_account_number(
                        account_id, lspu_account_id
                    )
                    _counter_uuid = self.get_counters(account_id, lspu_account_id)[
                        counter_id
                    ].get(ATTR_UUID)
                    assert _counter_uuid
                    if device.identifiers == {
                        (DOMAIN, make_device_id(_account_number, _counter_uuid))
                    }:
                        return account_id, lspu_account_id, counter_id

        return None, None, None

    @async_api_request_handler
    async def _async_get_client_info(self) -> dict[str, Any]:
        """Fetch client info."""
        return await self._api.async_get_client_info()

    @async_api_request_handler
    async def _async_get_accounts(self) -> dict[str, Any]:
        """Fetch accounts info."""
        return await self._api.async_get_accounts()

    @async_api_request_handler
    async def _async_get_els_info(self, els_id: int) -> dict[str, Any]:
        """Fetch els info."""
        return await self._api.async_get_els_info(els_id)

    @async_api_request_handler
    async def _async_get_lspu_info(self, lspu_id: int) -> dict[str, Any]:
        """Fetch lspu info."""
        return await self._api.async_get_lspu_info(lspu_id)

    @async_api_request_handler
    async def _async_get_charges(self, lspu_id: int) -> dict[str, Any]:
        """Fetch charges info."""
        return await self._api.async_get_charges(lspu_id)

    @async_api_request_handler
    async def _async_get_payments(self, lspu_id: int) -> dict[str, Any]:
        """Fetch payments info."""
        return await self._api.async_get_payments(lspu_id)

    @async_api_request_handler
    async def _async_send_readings(
        self,
        lspu_id: int,
        equipment_uuid: str,
        value: float,
        els_id: int | None = None,
    ) -> list[dict[str, Any]]:
        """Send readings with handle errors by decorator."""
        return await self._api.async_indication_send(
            lspu_id, equipment_uuid, value, els_id
        )

    @async_api_request_handler
    async def _async_get_receipt(
        self, date_iso_short: str, email: str | None, account_number: int, is_els: bool
    ) -> dict[str, Any]:
        """Get receipt data."""
        return await self._api.async_get_receipt(
            date_iso_short,
            email,  # pyright: ignore[reportArgumentType]
            account_number,
            is_els,
        )

    async def async_get_bill(
        self,
        device_id: str,
        bill_date: date | None = None,
        email: str | None = None,
    ) -> dict[str, Any] | None:
        """Get receipt data."""
        if bill_date is None:
            bill_date = date.today()
        date_iso_short = bill_date.strftime("%Y-%m-%d")
        device = await self.find_account_by_device_id(device_id)
        assert device
        account_id, *_ = device
        if account_id is not None:
            is_els = self.is_els()
            return await self._async_get_receipt(
                date_iso_short, email, account_id, is_els
            )
        return None

    async def async_send_readings(
        self,
        device_id,
        value: float,
    ) -> list[dict[str, Any]]:
        """Send readings with handle errors by decorator."""
        device = await self.find_account_by_device_id(device_id)
        assert device
        account_id, lspu_account_id, counter_id = device
        assert account_id is not None
        assert counter_id is not None
        assert lspu_account_id is not None
        lspu_accounts = self.get_lspu_accounts(account_id)
        assert lspu_accounts
        lspu_account = lspu_accounts[lspu_account_id]
        assert lspu_account
        lspu_id = lspu_account[ATTR_ACCOUNT_ID]

        if self.is_els():
            els_id = account_id
        else:
            els_id = None

        counters = self.get_counters(account_id, lspu_account_id)
        assert counters
        equipment_uuid = counters[counter_id][ATTR_UUID]
        assert equipment_uuid

        return await self._async_send_readings(
            lspu_id,
            equipment_uuid,
            value,
            els_id,
        )
