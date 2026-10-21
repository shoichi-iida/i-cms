# -*- coding: utf-8 -*-
"""
パスワードのハッシュ化・照合（UtilEncrypt）の単体テスト

対応仕様書: docs/単体テスト仕様書/パスワードのハッシュ化（UtilEncrypt）.md
"""

import os
import sys
from unittest import TestCase

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from api_test_helper import TEST_ENCRYPT_KEY  # noqa: F401  bcrypt_rounds等のoptions定義のため
from functions.common.util_encrypt import UtilEncrypt

PASSWORD = "P@ssw0rd"


class TestHashPassword(TestCase):

    def test_01_hash_is_bcrypt_and_not_plain(self):
        """正常系: bcryptハッシュ（$2b$始まり・60文字）が返り、平文は残らない"""
        hashed = UtilEncrypt.hash_password(PASSWORD)
        self.assertTrue(hashed.startswith("$2b$"))
        self.assertEqual(len(hashed), 60)
        self.assertNotEqual(hashed, PASSWORD)
        self.assertNotIn(PASSWORD, hashed)

    def test_02_hash_is_salted(self):
        """正常系: 同じパスワードでも毎回異なるハッシュになる（ソルト付き）"""
        self.assertNotEqual(
            UtilEncrypt.hash_password(PASSWORD),
            UtilEncrypt.hash_password(PASSWORD),
        )

    def test_03_verify_matches_correct_password(self):
        """正常系: 正しいパスワードで照合が成功する"""
        self.assertTrue(
            UtilEncrypt.verify_password(PASSWORD, UtilEncrypt.hash_password(PASSWORD)))

    def test_04_verify_rejects_wrong_password(self):
        """異常系: 誤ったパスワードでは照合が失敗する"""
        hashed = UtilEncrypt.hash_password(PASSWORD)
        for wrong in ("p@ssw0rd", "P@ssw0rd ", "P@ssw0r", ""):
            self.assertFalse(UtilEncrypt.verify_password(wrong, hashed))

    def test_05_verify_rejects_xor_encrypted_value(self):
        """
        異常系: XOR暗号のまま保存された値では照合できない

        移行スクリプト（migrate_password_to_bcrypt.py）の実行が必須であることの明示。
        """
        xor_value = UtilEncrypt.encrypt_xor(PASSWORD, TEST_ENCRYPT_KEY)
        self.assertFalse(UtilEncrypt.verify_password(PASSWORD, xor_value))

    def test_06_verify_rejects_empty_hash(self):
        """異常系: ハッシュが空・None・不正形式の場合は照合が失敗する"""
        for hashed in ("", None, "not-a-hash"):
            self.assertFalse(UtilEncrypt.verify_password(PASSWORD, hashed))

    def test_07_hash_empty_password_returns_as_is(self):
        """正常系: 空のパスワードはハッシュ化せずそのまま返す（パスワード未設定アカウント）"""
        self.assertEqual(UtilEncrypt.hash_password(""), "")
        self.assertIsNone(UtilEncrypt.hash_password(None))

    def test_08_is_hashed_password(self):
        """正常系: bcryptハッシュ済みかを判定できる"""
        self.assertTrue(
            UtilEncrypt.is_hashed_password(UtilEncrypt.hash_password(PASSWORD)))
        self.assertTrue(UtilEncrypt.is_hashed_password("$2a$12$" + "x" * 53))
        self.assertTrue(UtilEncrypt.is_hashed_password("$2y$12$" + "x" * 53))
        self.assertFalse(
            UtilEncrypt.is_hashed_password(
                UtilEncrypt.encrypt_xor(PASSWORD, TEST_ENCRYPT_KEY)))
        self.assertFalse(UtilEncrypt.is_hashed_password(""))
        self.assertFalse(UtilEncrypt.is_hashed_password(None))

    def test_09_multibyte_password_over_72_bytes_is_accepted(self):
        """
        正常系: 72バイトを超える多バイトパスワードでもハッシュ化・照合できる

        パスワード規定（8文字以上50文字以下）は文字数での制限のため、
        全角のみのパスワードは72バイト（bcryptの上限）を超えうる。
        hash_password・verify_passwordが同じ切り詰めを行うため照合は成功する。
        """
        password = "あ" * 46 + "Aa1!"
        self.assertGreater(len(password.encode("utf-8")), UtilEncrypt.BCRYPT_MAX_BYTES)
        hashed = UtilEncrypt.hash_password(password)
        self.assertTrue(UtilEncrypt.verify_password(password, hashed))

    def test_10_truncate_password_limits_to_72_bytes(self):
        """正常系: 72バイトを超える入力はUTF-8で72バイトへ切り詰められる"""
        self.assertEqual(
            UtilEncrypt.truncate_password("a" * 100), b"a" * 72)
        self.assertEqual(
            len(UtilEncrypt.truncate_password("あ" * 50)),
            UtilEncrypt.BCRYPT_MAX_BYTES,
        )

    def test_11_bcrypt_rounds_is_taken_from_options(self):
        """正常系: コストファクタは設定（bcrypt_rounds）から取得され、有効範囲に丸められる"""
        from tornado.options import options
        original = options.bcrypt_rounds
        try:
            options.bcrypt_rounds = 5
            self.assertEqual(UtilEncrypt.get_bcrypt_rounds(), 5)
            # 有効範囲（4〜31）外は丸める
            options.bcrypt_rounds = 1
            self.assertEqual(
                UtilEncrypt.get_bcrypt_rounds(), UtilEncrypt.BCRYPT_MIN_ROUNDS)
            options.bcrypt_rounds = 99
            self.assertEqual(
                UtilEncrypt.get_bcrypt_rounds(), UtilEncrypt.BCRYPT_MAX_ROUNDS)
        finally:
            options.bcrypt_rounds = original

    def test_12_xor_round_trip_is_kept_for_migration(self):
        """正常系: XOR暗号・復号は移行用に残されている"""
        encrypted = UtilEncrypt.encrypt_xor(PASSWORD, TEST_ENCRYPT_KEY)
        self.assertNotEqual(encrypted, PASSWORD)
        self.assertEqual(
            UtilEncrypt.decrypt_xor(encrypted, TEST_ENCRYPT_KEY), PASSWORD)
