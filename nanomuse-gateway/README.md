# nanoMuse Gateway 已迁至独立仓库

电脑端网关现在独立维护于 [BoardWare-Genius/nanomuse-gateway](https://github.com/BoardWare-Genius/nanomuse-gateway)。此目录只保留迁移说明；网关代码、依赖、配置示例、测试与 CI 均在独立仓库中。

## 安装与启动

在与 muse-gadget-sdk 并列的目录下载网关：

```bash
git clone https://github.com/BoardWare-Genius/nanomuse-gateway.git
cd nanomuse-gateway
uv sync --locked
# 首次配置；已有 config.toml 时跳过此命令。
uv run --locked nanomuse-gateway --init-config
```

从旧部署迁移时，先备份并复制本机 `.env`、`config.toml`、`.state/`，以及启用保存时的 `recordings/` 到独立网关目录。通知缓存包含未读与去重状态，应在旧网关停止后复制。不要复制 `.venv/`，在新目录运行 `uv sync --locked` 重建环境；保留凭据文件的私有权限，不要提交到 Git。

停止旧进程后，在独立网关目录启动：

```bash
uv run --locked --env-file .env nanomuse-gateway --config config.toml
```

保留原监听地址、端口和设备密钥时，CoreS3 可自动重连，无需重新烧录。配置中的录音和通知缓存相对路径按启动工作目录解析；使用绝对路径时请检查是否仍指向旧目录。

完整安装、ASR/TTS、审批、通知、图片和协议说明见独立仓库的 [README](https://github.com/BoardWare-Genius/nanomuse-gateway#readme)。

## CoreS3 固件

固件仍在 muse-gadget-sdk 维护。在 **muse-gadget-sdk 仓库根目录**使用 ESP-IDF v6.0.1：

```bash
esp32/tools/nanomuse.sh menuconfig
esp32/tools/nanomuse.sh build
# 按实际串口替换端口。
esp32/tools/nanomuse.sh -p /dev/ttyACM0 flash monitor
```

网关与固件继续使用协议 v1；审批、通知、图片和流式音频通过设备能力字段协商。单纯迁移网关目录不改变设备协议。
