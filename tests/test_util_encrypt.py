# -*- coding: utf-8 -*-
"""
パスワードのハッシュ化・照合（UtilEncrypt）の単体テスト

対応仕様書: docs/単体テスト仕様書/パスワードのハッシュ化（UtilEncrypt）.md
"""

import os
import sys
from unittest import TestCase

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import bcrypt
from tornado.options import define, options

from functions.common.util_encrypt import UtilEncrypt

PASSWORD = "P@ssw0rd"

# XOR暗号（移行用）の検証に使う暗号化キー
TEST_ENCRYPT_KEY = "i-cms-test"

# bcryptのコストファクタを最小にしてテストの実行時間を抑える
# （LoadOptions.define_optionsを呼ばずに単体で実行できるよう、未定義の場合だけ定義する）
if "bcrypt_rounds" not in options:
    define("bcrypt_rounds", default=12, type=int)
options.bcrypt_rounds = UtilEncrypt.BCRYPT_MIN_ROUNDS


class TestHashPassword(TestCase):

    def test_01_hash_is_bcrypt_and_not_plain(self):
        """正常系: bcrypt-sha256形式（プレフィクス＋60文字のbcryptハッシュ）が返り、平文は残らない"""
        hashed = UtilEncrypt.hash_password(PASSWORD)
        self.assertTrue(hashed.startswith(UtilEncrypt.BCRYPT_SHA256_PREFIX + "$2b$"))
        self.assertEqual(
            len(hashed), len(UtilEncrypt.BCRYPT_SHA256_PREFIX) + 60)
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

        移行スクリプト（setup/migrate_data.py）の実行が必須であることの明示。
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
        """正常系: ハッシュ済み（bcrypt-sha256・旧形式のbcrypt）かを判定できる"""
        self.assertTrue(
            UtilEncrypt.is_hashed_password(UtilEncrypt.hash_password(PASSWORD)))
        self.assertTrue(UtilEncrypt.is_hashed_password("$2b$12$" + "x" * 53))
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
        SHA-256で事前ハッシュしてからbcryptへ渡すため照合は成功する。
        """
        password = "あ" * 46 + "Aa1!"
        self.assertGreater(len(password.encode("utf-8")), UtilEncrypt.BCRYPT_MAX_BYTES)
        hashed = UtilEncrypt.hash_password(password)
        self.assertTrue(UtilEncrypt.verify_password(password, hashed))

    def test_10_suffix_beyond_72_bytes_changes_the_hash(self):
        """
        異常系: 72バイトより後ろだけが異なるパスワードでは照合が失敗する

        bcryptは72バイトまでしか扱わないため、切り詰めると末尾だけが異なる
        パスワードが同じハッシュになる。パスワード全体を事前ハッシュしていることの確認。
        """
        password = "あ" * 24 + "Aa1!"
        other = "あ" * 24 + "Bb2@"
        self.assertEqual(
            password.encode("utf-8")[:UtilEncrypt.BCRYPT_MAX_BYTES],
            other.encode("utf-8")[:UtilEncrypt.BCRYPT_MAX_BYTES])
        hashed = UtilEncrypt.hash_password(password)
        self.assertTrue(UtilEncrypt.verify_password(password, hashed))
        self.assertFalse(UtilEncrypt.verify_password(other, hashed))

    def test_10_2_prehash_password_fits_bcrypt_limit(self):
        """正常系: 事前ハッシュは長さによらず44バイトで、bcryptの上限（72バイト）に収まる"""
        for plain in ("a", "a" * 100, "あ" * 50):
            prehashed = UtilEncrypt.prehash_password(plain)
            self.assertEqual(len(prehashed), 44)
            self.assertNotIn(b"\x00", prehashed)

    def test_10_3_verify_legacy_bcrypt_hash(self):
        """
        正常系・異常系: 旧形式（事前ハッシュなしのbcrypt）は72バイト以内のパスワードに限り照合する

        72バイトを超えるパスワードは、切り詰めると末尾だけが異なるパスワードでも
        一致してしまうため照合しない。
        """
        legacy = bcrypt.hashpw(PASSWORD.encode("utf-8"), bcrypt.gensalt(rounds=4)).decode("ascii")
        self.assertTrue(UtilEncrypt.verify_password(PASSWORD, legacy))
        self.assertFalse(UtilEncrypt.verify_password("p@ssw0rd", legacy))
        long_password = "a" * 73
        legacy_long = bcrypt.hashpw(
            long_password.encode("utf-8")[:UtilEncrypt.BCRYPT_MAX_BYTES],
            bcrypt.gensalt(rounds=4)).decode("ascii")
        self.assertFalse(UtilEncrypt.verify_password(long_password, legacy_long))

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
