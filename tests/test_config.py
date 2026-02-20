"""
Test per config.py — secrets multi-sorgente, .env loader, YAML parser.
Zero dipendenze: stdlib solo.
"""

from __future__ import print_function

import os
import sys
import tempfile
import shutil

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import (
    _load_dotenv, _resolve_env, _resolve_file, _resolve_cmd,
    _expand_secrets, _walk_expand, _parse_yaml_manual, load_config
)


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _tmp_file(content, suffix=".txt"):
    # type: (str, str) -> str
    """Crea un file temporaneo con il contenuto dato. Ritorna il path."""
    fd, path = tempfile.mkstemp(suffix=suffix)
    os.write(fd, content.encode("utf-8"))
    os.close(fd)
    return path


def _cleanup(*paths):
    for p in paths:
        if os.path.exists(p):
            os.unlink(p)


# ---------------------------------------------------------------------------
# Test: .env loader
# ---------------------------------------------------------------------------

def test_dotenv_loads_variables():
    env_file = _tmp_file("TEST_HADOOPSCOPE_VAR=hello_world\n# comment\nANOTHER=foo\n")
    try:
        # Assicura che non esista nell'ambiente
        os.environ.pop("TEST_HADOOPSCOPE_VAR", None)
        os.environ.pop("ANOTHER", None)
        _load_dotenv(env_file)
        assert os.environ.get("TEST_HADOOPSCOPE_VAR") == "hello_world", \
            "Expected 'hello_world', got: {}".format(os.environ.get("TEST_HADOOPSCOPE_VAR"))
        assert os.environ.get("ANOTHER") == "foo"
    finally:
        os.environ.pop("TEST_HADOOPSCOPE_VAR", None)
        os.environ.pop("ANOTHER", None)
        _cleanup(env_file)


def test_dotenv_does_not_overwrite_existing():
    env_file = _tmp_file("TEST_EXISTING_VAR=from_file\n")
    try:
        os.environ["TEST_EXISTING_VAR"] = "from_shell"
        _load_dotenv(env_file)
        assert os.environ.get("TEST_EXISTING_VAR") == "from_shell", \
            "Should NOT overwrite shell env var"
    finally:
        os.environ.pop("TEST_EXISTING_VAR", None)
        _cleanup(env_file)


def test_dotenv_strips_quotes():
    env_file = _tmp_file('QUOTED_VAR="my secret"\nSINGLE_QUOTED=\'other\'\n')
    try:
        os.environ.pop("QUOTED_VAR", None)
        os.environ.pop("SINGLE_QUOTED", None)
        _load_dotenv(env_file)
        assert os.environ.get("QUOTED_VAR") == "my secret"
        assert os.environ.get("SINGLE_QUOTED") == "other"
    finally:
        os.environ.pop("QUOTED_VAR", None)
        os.environ.pop("SINGLE_QUOTED", None)
        _cleanup(env_file)


def test_dotenv_missing_file_is_silent():
    _load_dotenv("/nonexistent/.env")  # deve silenziosamente non fare nulla


# ---------------------------------------------------------------------------
# Test: _resolve_env
# ---------------------------------------------------------------------------

def test_resolve_env_present():
    os.environ["HS_TEST_SECRET"] = "mypassword"
    try:
        assert _resolve_env("HS_TEST_SECRET") == "mypassword"
    finally:
        del os.environ["HS_TEST_SECRET"]


def test_resolve_env_missing_raises():
    os.environ.pop("HS_TEST_MISSING_VAR", None)
    try:
        _resolve_env("HS_TEST_MISSING_VAR")
        assert False, "Should have raised"
    except ValueError as e:
        assert "HS_TEST_MISSING_VAR" in str(e)


# ---------------------------------------------------------------------------
# Test: _resolve_file
# ---------------------------------------------------------------------------

def test_resolve_file_reads_content():
    secret_file = _tmp_file("  supersecret  \n")
    try:
        assert _resolve_file(secret_file) == "supersecret"
    finally:
        _cleanup(secret_file)


def test_resolve_file_missing_raises():
    try:
        _resolve_file("/nonexistent/secret/file")
        assert False, "Should have raised"
    except ValueError as e:
        assert "not found" in str(e)


def test_resolve_file_empty_raises():
    empty_file = _tmp_file("")
    try:
        _resolve_file(empty_file)
        assert False, "Should have raised"
    except ValueError as e:
        assert "empty" in str(e)
    finally:
        _cleanup(empty_file)


# ---------------------------------------------------------------------------
# Test: _resolve_cmd
# ---------------------------------------------------------------------------

def test_resolve_cmd_echo():
    result = _resolve_cmd("echo mysecret")
    assert result == "mysecret", "Got: {}".format(result)


def test_resolve_cmd_pipe():
    # Comando con pipe — supportato via shell=True
    result = _resolve_cmd("echo 'hello world' | tr '[:lower:]' '[:upper:]'")
    assert "HELLO" in result


def test_resolve_cmd_failure_raises():
    try:
        _resolve_cmd("false")  # exit code 1
        assert False, "Should have raised"
    except ValueError as e:
        assert "exit code" in str(e) or "exited with code" in str(e)


def test_resolve_cmd_empty_output_raises():
    try:
        _resolve_cmd("true")  # exit 0 ma nessun output
        assert False, "Should have raised"
    except ValueError as e:
        assert "no output" in str(e)


# ---------------------------------------------------------------------------
# Test: _expand_secrets (pattern matching)
# ---------------------------------------------------------------------------

def test_expand_env_var():
    os.environ["HS_TEST_EXPAND"] = "expanded_value"
    try:
        result = _expand_secrets("${HS_TEST_EXPAND}")
        assert result == "expanded_value"
    finally:
        del os.environ["HS_TEST_EXPAND"]


def test_expand_file_pattern():
    secret_file = _tmp_file("filepassword")
    try:
        result = _expand_secrets("${{file:{}}}".format(secret_file))
        assert result == "filepassword"
    finally:
        _cleanup(secret_file)


def test_expand_cmd_pattern():
    result = _expand_secrets("${cmd:echo cmdpassword}")
    assert result == "cmdpassword"


def test_expand_multiple_patterns():
    os.environ["HS_HOST"] = "ambari.corp.com"
    os.environ["HS_PORT"] = "8080"
    try:
        result = _expand_secrets("http://${HS_HOST}:${HS_PORT}/api")
        assert result == "http://ambari.corp.com:8080/api"
    finally:
        del os.environ["HS_HOST"]
        del os.environ["HS_PORT"]


def test_expand_no_pattern_passthrough():
    assert _expand_secrets("plain string") == "plain string"
    assert _expand_secrets(42) == 42
    assert _expand_secrets(True) is True


# ---------------------------------------------------------------------------
# Test: load_config con .env auto-load
# ---------------------------------------------------------------------------

def test_load_config_with_dotenv():
    """Verifica che load_config legga il .env automaticamente."""
    tmpdir = tempfile.mkdtemp()
    try:
        # Scrivi .env
        env_path = os.path.join(tmpdir, ".env")
        with open(env_path, 'w') as f:
            f.write("HS_TEST_AMBARI_PASS=from_dotenv\n")

        # Scrivi config
        cfg_path = os.path.join(tmpdir, "test.yaml")
        with open(cfg_path, 'w') as f:
            f.write("""
version: "1"
environments:
  test:
    type: hdp
    ambari_url: http://localhost:8080
    ambari_user: admin
    ambari_pass: "${HS_TEST_AMBARI_PASS}"
    cluster_name: test
""")
        # Assicura che la var non sia nel shell env
        os.environ.pop("HS_TEST_AMBARI_PASS", None)

        cfg = load_config(cfg_path)
        assert cfg["environments"]["test"]["ambari_pass"] == "from_dotenv", \
            "Expected 'from_dotenv', got: {}".format(
                cfg["environments"]["test"]["ambari_pass"])
    finally:
        os.environ.pop("HS_TEST_AMBARI_PASS", None)
        shutil.rmtree(tmpdir)


def test_load_config_file_secret():
    """Verifica ${file:/path} in config."""
    tmpdir = tempfile.mkdtemp()
    try:
        secret_file = os.path.join(tmpdir, "ambari_pass")
        with open(secret_file, 'w') as f:
            f.write("filereadsecret\n")

        cfg_path = os.path.join(tmpdir, "test.yaml")
        with open(cfg_path, 'w') as f:
            f.write("""
version: "1"
environments:
  test:
    type: hdp
    ambari_url: http://localhost:8080
    ambari_user: admin
    ambari_pass: "${{file:{}}}"
    cluster_name: test
""".format(secret_file))

        cfg = load_config(cfg_path)
        assert cfg["environments"]["test"]["ambari_pass"] == "filereadsecret"
    finally:
        shutil.rmtree(tmpdir)


def test_load_config_cmd_secret():
    """Verifica ${cmd:...} in config."""
    tmpdir = tempfile.mkdtemp()
    try:
        cfg_path = os.path.join(tmpdir, "test.yaml")
        with open(cfg_path, 'w') as f:
            f.write("""
version: "1"
environments:
  test:
    type: hdp
    ambari_url: http://localhost:8080
    ambari_user: admin
    ambari_pass: "${cmd:echo cmdsecret}"
    cluster_name: test
""")
        cfg = load_config(cfg_path)
        assert cfg["environments"]["test"]["ambari_pass"] == "cmdsecret"
    finally:
        shutil.rmtree(tmpdir)


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    tests = [
        test_dotenv_loads_variables,
        test_dotenv_does_not_overwrite_existing,
        test_dotenv_strips_quotes,
        test_dotenv_missing_file_is_silent,
        test_resolve_env_present,
        test_resolve_env_missing_raises,
        test_resolve_file_reads_content,
        test_resolve_file_missing_raises,
        test_resolve_file_empty_raises,
        test_resolve_cmd_echo,
        test_resolve_cmd_pipe,
        test_resolve_cmd_failure_raises,
        test_resolve_cmd_empty_output_raises,
        test_expand_env_var,
        test_expand_file_pattern,
        test_expand_cmd_pattern,
        test_expand_multiple_patterns,
        test_expand_no_pattern_passthrough,
        test_load_config_with_dotenv,
        test_load_config_file_secret,
        test_load_config_cmd_secret,
    ]
    failed = 0
    for t in tests:
        try:
            t()
            print("PASS  {}".format(t.__name__))
        except Exception as e:
            print("FAIL  {} — {}".format(t.__name__, e))
            import traceback
            traceback.print_exc()
            failed += 1
    print("\n{}/{} passed".format(len(tests) - failed, len(tests)))
    sys.exit(failed)
