"""Layer Ops — tool on-demand (non schedulati), affiancati al monitoring."""


def build_ops_registry():
    # type: () -> dict
    """Registry dei tool Ops disponibili, per nome."""
    from ops.yarn_app import AppStatusTool, AppLogsTool
    tools = [AppStatusTool, AppLogsTool]
    return {cls.name: cls for cls in tools}
