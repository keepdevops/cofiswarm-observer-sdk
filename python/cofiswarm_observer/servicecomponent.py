"""Canonical async NATS service component for cofiswarm Python components — the Python port of
cofiswarm-observer-sdk/pkg/servicecomponent. Announces presence on the observer bus, serves
capability subjects behind the schema-major gate with loud-error replies, re-announces on
hello, and says goodbye on shutdown.

A route handler is an async callable taking the decoded request dict and returning a reply
dict; schema_version and ok are filled in automatically if the handler omits them.
"""
from __future__ import annotations

import json
import logging
from typing import Awaitable, Callable

import nats

from . import contract

logger = logging.getLogger("cofiswarm.observer")

Handler = Callable[[dict], Awaitable[dict]]


class ServiceComponent:
    """A generic bus capability process: announce + serve subjects + hello/goodbye."""

    def __init__(self, nc, name: str, kind: str, routes: dict[str, Handler]):
        self._nc = nc
        self._name = name
        self._kind = kind
        self._cid = kind  # component_id defaults to kind, mirroring the Go component
        self._routes = routes
        self._primary = sorted(routes)[0] if routes else ""

    @classmethod
    async def connect(cls, url: str, name: str):
        """Open a NATS connection with infinite reconnect (broker-bounce resilience)."""
        return await nats.connect(url, name=name, max_reconnect_attempts=-1)

    async def start(self) -> None:
        """Subscribe the routes plus hello (for re-announce) and publish the initial announce."""
        for subject, handler in self._routes.items():
            await self._nc.subscribe(subject, cb=self._make_cb(subject, handler))
        await self._nc.subscribe(contract.SUBJ_HELLO, cb=self._on_hello)
        await self._announce()
        logger.info(
            "%s (%s) serving %d subjects (id=%s)",
            self._name, self._kind, len(self._routes), self._cid,
        )

    def _make_cb(self, subject: str, handler: Handler):
        async def cb(msg) -> None:
            try:
                raw = json.loads(msg.data)
            except (ValueError, TypeError) as exc:
                logger.error("%s dropped malformed message on %s: %s", self._name, subject, exc)
                await self._reply_err(msg, "invalid json")
                return
            if not contract.major_supported(raw):
                logger.error("%s rejected %s: unsupported schema_version", self._name, subject)
                await self._reply_err(msg, "unsupported schema_version")
                return
            try:
                reply = await handler(raw)
            except Exception as exc:  # loud: a handler failure replies an error, never silent
                logger.error("%s handler %s failed: %s", self._name, subject, exc)
                await self._reply_err(msg, str(exc))
                return
            reply.setdefault("schema_version", contract.SCHEMA_VERSION)
            reply.setdefault("ok", True)
            await self._respond(msg, reply)

        return cb

    async def _on_hello(self, msg) -> None:
        logger.info("%s re-announcing on hello", self._name)
        await self._announce()

    async def _reply_err(self, msg, message: str) -> None:
        await self._respond(msg, {
            "schema_version": contract.SCHEMA_VERSION, "ok": False, "error": message,
        })

    async def _respond(self, msg, payload: dict) -> None:
        if not getattr(msg, "reply", ""):
            return
        try:
            await self._nc.publish(msg.reply, json.dumps(payload).encode())
        except Exception as exc:
            logger.error("%s failed to respond on %s: %s", self._name, msg.subject, exc)

    async def _announce(self) -> None:
        announce = {
            "schema_version": contract.SCHEMA_VERSION,
            "component_id": self._cid,
            "kind": self._kind,
            "info": {"name": self._name, "engine": self._kind, "tags": ["resource"]},
            "infer_subject": self._primary,
        }
        try:
            await self._nc.publish(contract.SUBJ_ANNOUNCE, json.dumps(announce).encode())
        except Exception as exc:
            logger.error("%s announce failed: %s", self._name, exc)

    async def shutdown(self) -> None:
        """Publish a graceful goodbye so presence flips offline quietly."""
        goodbye = {
            "schema_version": contract.SCHEMA_VERSION, "component_id": self._cid, "reason": "shutdown",
        }
        try:
            await self._nc.publish(contract.SUBJ_GOODBYE, json.dumps(goodbye).encode())
            await self._nc.flush()
        except Exception as exc:
            logger.error("%s goodbye failed: %s", self._name, exc)
