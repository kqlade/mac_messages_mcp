"""
Tests for macOS 26 (Tahoe) compatibility: chat-GUID resolution and send paths.
"""

import unittest
from unittest.mock import MagicMock, patch

from mac_messages_mcp.messages import (
    _find_existing_chat_guid,
    _resolve_group_chat_guid,
    _send_message_to_recipient,
)


class TestFindExistingChatGuid(unittest.TestCase):
    """Tests for _find_existing_chat_guid."""

    @patch("mac_messages_mcp.messages.query_messages_db")
    def test_returns_guid_for_phone(self, mock_db):
        mock_db.return_value = [{"guid": "any;-;+14155551234"}]
        guid = _find_existing_chat_guid("+14155551234")
        self.assertEqual(guid, "any;-;+14155551234")
        # Query must include phone format variants as parameters.
        params = mock_db.call_args[0][1]
        self.assertIn("+14155551234", params)

    @patch("mac_messages_mcp.messages.query_messages_db")
    def test_returns_guid_for_email(self, mock_db):
        mock_db.return_value = [{"guid": "iMessage;-;ola@example.com"}]
        guid = _find_existing_chat_guid("ola@example.com")
        self.assertEqual(guid, "iMessage;-;ola@example.com")

    @patch("mac_messages_mcp.messages.query_messages_db")
    def test_returns_none_when_no_chat(self, mock_db):
        mock_db.return_value = []
        self.assertIsNone(_find_existing_chat_guid("+14155551234"))

    @patch("mac_messages_mcp.messages.query_messages_db")
    def test_returns_none_on_db_error(self, mock_db):
        mock_db.return_value = [{"error": "locked"}]
        self.assertIsNone(_find_existing_chat_guid("+14155551234"))

    def test_returns_none_for_empty_input(self):
        self.assertIsNone(_find_existing_chat_guid(""))


class TestResolveGroupChatGuid(unittest.TestCase):
    """Tests for _resolve_group_chat_guid."""

    @patch("mac_messages_mcp.messages.query_messages_db")
    def test_bare_legacy_identifier(self, mock_db):
        mock_db.return_value = [{"guid": "iMessage;+;chat123456"}]
        guid = _resolve_group_chat_guid("chat123456")
        self.assertEqual(guid, "iMessage;+;chat123456")

    @patch("mac_messages_mcp.messages.query_messages_db")
    def test_bare_tahoe_hex_identifier(self, mock_db):
        mock_db.return_value = [{"guid": "any;+;e63dec85e7d34dfe9628263b1736d488"}]
        guid = _resolve_group_chat_guid("e63dec85e7d34dfe9628263b1736d488")
        self.assertEqual(guid, "any;+;e63dec85e7d34dfe9628263b1736d488")

    @patch("mac_messages_mcp.messages.query_messages_db")
    def test_stale_prefixed_guid_falls_back_to_bare(self, mock_db):
        # First lookup (stale full GUID) misses; bare-identifier retry hits.
        mock_db.side_effect = [
            [],
            [{"guid": "any;+;chat123456"}],
        ]
        guid = _resolve_group_chat_guid("iMessage;+;chat123456")
        self.assertEqual(guid, "any;+;chat123456")
        self.assertEqual(mock_db.call_count, 2)

    @patch("mac_messages_mcp.messages.query_messages_db")
    def test_unknown_identifier_returns_none(self, mock_db):
        mock_db.return_value = []
        self.assertIsNone(_resolve_group_chat_guid("chat999"))


class TestTahoeSendPaths(unittest.TestCase):
    """Tests for _send_message_to_recipient strategy ordering."""

    @patch("mac_messages_mcp.messages.run_applescript")
    @patch("mac_messages_mcp.messages._find_existing_chat_guid")
    def test_existing_chat_sends_by_chat_id(self, mock_guid, mock_run):
        mock_guid.return_value = "any;-;+14155551234"
        mock_run.return_value = ""  # success

        result = _send_message_to_recipient("+14155551234", "hello")

        self.assertIn("successfully", result)
        first_command = mock_run.call_args_list[0][0][0]
        self.assertIn('chat id "any;-;+14155551234"', first_command)

    @patch("mac_messages_mcp.messages._get_send_service_ids")
    @patch("mac_messages_mcp.messages._macos_major_version")
    @patch("mac_messages_mcp.messages.run_applescript")
    @patch("mac_messages_mcp.messages._find_existing_chat_guid")
    def test_new_conversation_on_tahoe_uses_service_id(
        self, mock_guid, mock_run, mock_ver, mock_services
    ):
        mock_guid.return_value = None
        mock_ver.return_value = 26
        mock_services.return_value = ["92E6DCC3-13E0-44E5-B396-997B9FF616EF"]

        def applescript_result(command):
            if "service type = iMessage" in command:
                return "Error: -1728"
            return ""  # service-id path succeeds

        mock_run.side_effect = applescript_result

        result = _send_message_to_recipient("+14155559999", "hello")

        self.assertIn("successfully", result)
        commands = [c[0][0] for c in mock_run.call_args_list]
        self.assertTrue(
            any(
                'service id "92E6DCC3-13E0-44E5-B396-997B9FF616EF"' in c
                for c in commands
            )
        )

    @patch("mac_messages_mcp.messages.run_applescript")
    @patch("mac_messages_mcp.messages._resolve_group_chat_guid")
    def test_group_send_resolves_bare_identifier(self, mock_resolve, mock_run):
        mock_resolve.return_value = "any;+;e63dec85e7d34dfe9628263b1736d488"
        mock_run.return_value = ""

        result = _send_message_to_recipient(
            "e63dec85e7d34dfe9628263b1736d488", "hello", group_chat=True
        )

        self.assertIn("successfully", result)
        first_command = mock_run.call_args_list[0][0][0]
        self.assertIn('chat id "any;+;e63dec85e7d34dfe9628263b1736d488"', first_command)


class TestToolGetChats(unittest.TestCase):
    """Tests for the group chat listing tool."""

    @patch("mac_messages_mcp.server.query_messages_db")
    def test_unnamed_groups_show_participants(self, mock_db):
        from mac_messages_mcp.server import tool_get_chats

        mock_db.return_value = [
            {
                "chat_identifier": "e63dec85e7d34dfe9628263b1736d488",
                "display_name": "",
                "participants": "+14155551111, +14155552222",
                "last_activity": 2,
            },
            {
                "chat_identifier": "chat123",
                "display_name": "Tech Groupchat",
                "participants": "+14155553333",
                "last_activity": 1,
            },
        ]

        result = tool_get_chats(MagicMock())

        self.assertIn("(unnamed: +14155551111, +14155552222)", result)
        self.assertIn("Tech Groupchat", result)
        self.assertIn("ID: e63dec85e7d34dfe9628263b1736d488", result)


if __name__ == "__main__":
    unittest.main()
