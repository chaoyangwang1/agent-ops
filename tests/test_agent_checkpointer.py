import pytest
import uuid
from src.agent.checkpointer import SessionManager


@pytest.fixture
def session_mgr():
    return SessionManager(
        pg_dsn="postgresql://agent:agent@localhost:5432/agentops",
        redis_url="redis://localhost:6379/1",
    )


def test_create_and_get_session(session_mgr):
    """创建和获取会话"""
    conv_id = str(uuid.uuid4())
    session_mgr.create_session(conv_id, {"checkpoint_id": "ckpt-1"})
    session = session_mgr.get_session(conv_id)
    assert session is not None
    assert session["checkpoint_id"] == "ckpt-1"
    # cleanup
    session_mgr.delete_session(conv_id)


def test_list_active_sessions(session_mgr):
    """列出活跃会话"""
    conv_id = str(uuid.uuid4())
    session_mgr.create_session(conv_id, {"checkpoint_id": "ckpt-2"})
    active = session_mgr.list_active_sessions()
    assert len(active) >= 1
    session_mgr.delete_session(conv_id)


def test_delete_session(session_mgr):
    """删除会话"""
    conv_id = str(uuid.uuid4())
    session_mgr.create_session(conv_id, {"checkpoint_id": "ckpt-3"})
    session_mgr.delete_session(conv_id)
    assert session_mgr.get_session(conv_id) is None
