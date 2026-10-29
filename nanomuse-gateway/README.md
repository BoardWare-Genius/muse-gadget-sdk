# CoreS3 接入局域网 nanoMuse

这一配置把 M5Stack CoreS3 当作 nanoMuse 的小终端：屏幕、触摸设置、头像、PWR 录音键继续使用。设备连接电脑上的网关，网关访问本机 nanoMuse。设备不再访问 `api.muse.ai`，不需要 Muse SDK token 或手机配对。

当前完成：USB 文字聊天、屏幕文字与图片回复、16 kHz 单声道录音上传、可选 WAV 保存、可选 ASR 识别与 TTS 播报、单次操作审批、后台通知、取消与断线处理。未配置 ASR 时只检查/保存录音，不会自动识别；未配置 TTS 时显示文字和图片。USB 文字聊天不播音频，按键录音的回复可以播报。

```text
CoreS3 ──Wi-Fi / WebSocket──> 电脑网关 ──HTTP──> 本机 nanoMuse
                               ├──> 可选 ASR 服务（录音转文字）
                               └──> 可选 TTS 服务（文字转语音）
```

## 1. 在电脑启动 nanoMuse

按 [nanoMuse 安装说明](https://github.com/nano-muse/nanoMuse)安装并配置模型，然后运行：

```bash
nanomuse serve
```

先在电脑打开 `http://127.0.0.1:8787`，确认可以正常文字聊天。nanoMuse 的模型可以使用你能够访问的服务；换成此终端不会自动解决模型服务本身的网络问题。

通过 Docker 部署时，将容器的 8787 端口发布到主机。其他电脑访问 `http://主机局域网IP:8787/`，不要使用容器内部 IP；例如主机地址为 `192.168.1.10` 时，访问 `http://192.168.1.10:8787/`。

网关适配的是 [nanoMuse 本地 REST API](https://github.com/nano-muse/nanoMuse/blob/4ba855cf1fbed336e02b12566fef19bac5ce65d7/nanomuse/server/api.py)：创建线程、发送文字、读取最终消息和追问、停止运行。未复制 nanoMuse 的程序代码。每次设备连接创建自己的线程，正常连续聊天保留上下文；取消正在执行的请求后，新请求使用新线程，避免旧结果串入。新版固件可以处理本次对话的单次操作审批；需要补充信息时，网关把追问作为回复传给设备，下一次文字或录音继续回答这个问题。网关不会自动批准操作。

## 2. 启动网关

需要 Python 3.11+ 和 [uv](https://docs.astral.sh/uv/getting-started/installation/)。在此仓库根目录执行：

```bash
cd nanomuse-gateway
uv sync --locked
# 首次运行复制配置；已有 config.toml 时保留它。
cp -n config.example.toml config.toml

# 自己选一个 16–127 位无空格 ASCII 密钥，设备端填同一个值。
export CORES3_DEVICE_KEY='replace-with-your-own-device-key'

# 填 nanoMuse 本地服务的 server token；输入时不回显。
read -rsp 'nanoMuse server token: ' NANOMUSE_SERVER_TOKEN
export NANOMUSE_SERVER_TOKEN
printf '\n'

uv run --locked gateway.py --config config.toml
```

`uv sync --locked` 按 `uv.lock` 安装依赖，`uv run` 使用项目的 `.venv`，无需手动激活虚拟环境。之后在 `nanomuse-gateway/` 目录设置上述环境变量，再执行启动命令即可。也可以把环境变量写入本机 `.env`，用 `uv run --locked --env-file .env gateway.py --config config.toml` 启动。

如果不使用 uv，仍可用 `python3 -m venv .venv`、`.venv/bin/pip install -r requirements.txt` 和 `.venv/bin/python gateway.py --config config.toml`。

nanoMuse server token 的位置是它的数据目录中的 `server_token`，也可以在 nanoMuse 的服务配置中指定。它与 `mgst_…` SDK token、模型 API key、上面的设备密钥是不同的凭据。**只把设备密钥填进 CoreS3**。`config.toml`、`.env`、`recordings/` 已忽略，不要把凭据提交到仓库。

确认电脑上 `curl http://127.0.0.1:8765/health` 返回 `{"ok":true,"protocol":1}`。这个检查只表示网关运行，不表示模型已经能回答。设备需要使用电脑的**局域网 IPv4 地址**（例如 `192.168.1.10`），不能用 `127.0.0.1`。让 CoreS3 与电脑连接到能够互通的网络，并允许设备访问电脑 TCP 8765；访客网络/AP 隔离会阻断连接。`ws://` 适用于可信局域网；跨不可信网络需用带可信证书的 TLS 反向代理提供 `wss://`。

如果开启 Clash/Mihomo 的 TUN 后设备断连，应把设备及 ASR/TTS 所在的实际局域网网段加入 TUN 的 `route-exclude-address`。例如这些服务都在 `172.19.*.*` 网络时，可添加 `172.19.0.0/16`。Clash Verge 的设置界面、全局扩展配置和当前运行配置可能不一致；保存后重启应用，并用 `ip route get 设备IP` 检查回包是否经过物理网卡和局域网路由器。代理配置属于本机设置，不随本仓库提交。

想保存录音，在 `config.toml` 取消这一行的注释再启动网关：

```toml
[gateway]
recordings_dir = "recordings"
recordings_keep = 20
```

默认不写录音文件；开启后保留最近 20 个 WAV，目录只用于本网关录音。WAV 为 16000 Hz / mono / 16-bit PCM，可用播放器直接检查。

## 3. 配置并构建 CoreS3 固件

使用 ESP-IDF **v6.0.1**，在仓库根目录执行：

```bash
esp32/tools/nanomuse.sh menuconfig
```

`nanomuse.sh` 会在自己的进程中激活 ESP-IDF 6.0.1，并使用其专用 Python，因此可从网关的 `.venv` 中执行。若电脑安装了多个 IDF，可指定 `IDF_EXPORT=/home/你的用户名/esp/esp-idf-v6/export.sh esp32/tools/nanomuse.sh build`。不要用指向 5.5.x 的 `get_idf` 来构建此项目，也不需要往网关虚拟环境中安装 `click` 或降级 IDF 6 的依赖。

在 `menuconfig` 中搜索（`/`）：

| 配置 | 填写内容 |
| --- | --- |
| `HOMEHUB_WIFI_SSID` | 2.4 GHz Wi-Fi 名称 |
| `HOMEHUB_WIFI_PASSWORD` | Wi-Fi 密码 |
| `MUSE_GATEWAY_URL` | `ws://电脑局域网IP:8765/device` |
| `MUSE_GATEWAY_KEY` | 与 `CORES3_DEVICE_KEY` 完全相同的设备密钥 |

```bash
esp32/tools/nanomuse.sh build
# 确认串口后再烧录，例：/dev/ttyACM0
esp32/tools/nanomuse.sh -p /dev/ttyACM0 flash monitor
```

输出在 `esp32/build-nanomuse-cores3/`，配置在该目录的 `sdkconfig`。原来的 `esp32/build/` 和官方 Muse 构建不受影响。此镜像保留原分区布局，不启用 Secure Boot 或 Flash 加密。编译后的固件包含编译时配置的 Wi-Fi 与设备密钥，请只保存在本机。

VS Code 可打开本机已生成的 `.vscode/nanomuse-cores3.code-workspace`；该文件是机器相关配置，被 Git 忽略。通用的构建入口是上面的 `tools/nanomuse.sh`。

请用 **File → Open Workspace from File…** 打开该文件，或在仓库根目录执行 `code -n .vscode/nanomuse-cores3.code-workspace`。仅在编辑器中打开此文件不会切换工作区。该工作区以 `esp32/` 为工程目录，使用 `build-nanomuse-cores3/` 和 nanoMuse 配置；若插件报 `CMakeLists.txt not found in project directory .../muse-gadget-sdk`，说明当前仍在仓库根目录运行。无需为此再次执行 Set Target。

烧录后，左滑进入设置，在 **Muse** 页面可更改网关 URL、设备密钥并点 Reconnect；Wi-Fi 页面可扫描/选择网络。这一模式记住一个 Wi-Fi，单独保存在设备的 Muse 设置中。NVS 中已保存的设置优先于编译默认值，换配置后请在屏幕或 USB 更新，不需要清空整片 Flash。

也可以在串口监视器中逐行输入以下命令（命令前的 `>` 是实际内容）：

```text
>wifi.ssid=你的WiFi名称
>wifi.pass=你的WiFi密码
>wifi.connect
>gateway.url=ws://192.168.1.10:8765/device
>gateway.key=你的设备密钥
>gateway.test
>chat=你好，请介绍一下你自己
```

USB 命令保存到 NVS；密码和设备密钥不在设备命令日志中回显。电脑的终端程序自身可能有本地回显。网关页面显示 Connected 表示设备与网关的协议连接成功；发送文字后才能验证 nanoMuse、模型配置与凭据是否正常。

新版固件会协商按句播报：网关先订阅 nanoMuse 的 `/ws` 实时文字，收到句号、问号、逗号等边界就调用本地 TTS；首段最多 24 字，后续最多 48 字；设备通过 `caption_chars` 上报更小的单屏容量时，以该容量为上限（CoreS3 中文字幕约 20 字），标点也不会绕过此限制。积累至少 6 字时，首段等待约 350ms、后续约 600ms 也会触发合成，无需等待句号。TTS 合成与模型生成并行，CoreS3 收到 PCM 后即可播放。VoxCPM 使用 `streaming=true`，增量解析 WAV 头并将后续 PCM 实时转换、发送，首段音频到达后即可播放；其他模型保留按短语请求完整 WAV 的兼容路径。若流式接口在发出音频前失败，会用普通 WAV 重试该短语；已发出部分音频后失败会提示错误，避免重复播报。`[tts] audio_streaming = false` 可关闭 VoxCPM 音频流。首段仍需等待识别、模型首段文字和 TTS 首块音频。若 nanoMuse 没有实时接口，自动回退到完整回答后合成；旧固件也保留原流程。

nanoMuse 配置将代码和只读数据保留在 Flash 中，PSRAM 继续用于 UI、录音和播放缓冲。这避免了本机 CoreS3 在启用 PSRAM XIP 时于 SPI/显示初始化发生的 `IllegalInstruction`。已有构建若仍启用 XIP，请在 `menuconfig` 中关闭 `SPIRAM_XIP_FROM_PSRAM`、`SPIRAM_FETCH_INSTRUCTIONS`、`SPIRAM_RODATA` 后重编译；已有 `sdkconfig` 优先于默认配置。

需要重启网关并重新烧录 CoreS3，两端更新后才启用按句播放。流式文字可能包含模型公开的任务进度，后续最终总结也可能调整此前说法。总音频仍限 60 秒，取消会停止当前合成任务并丢弃旧轮次音频。

串口发送 `>status` 可查询设备身份、连接状态和固件构建标识。nanoMuse 固件还会返回 `elf_sha256`（固件 ELF 哈希前缀）、`mode`、`mode_age_s` 与 `reply_phase`，用于排查屏幕状态：`mode` 的 1/2/3/4 分别表示待机/录音/等待/播放，`reply_phase` 的 0/1/2 分别表示等待回答/准备音频/阅读回复。只重启电脑网关不会更新开发板固件。

固件会在网关重启或连接断开后每隔约 5 秒尝试重连，收到网关的 `hello` 才恢复在线状态。旧固件若在网关重启后持续出现 `Websocket client is not connected` / `GATEWAY SEND FAILED`，先在设备设置中点 **Reconnect**，或通过串口发送 `>gateway.test` 恢复连接，再重新编译、烧录最新 nanoMuse 固件以启用自动恢复。

## 4. 怎么用

- **文字**：串口发送 `>chat=你好`。回复打印为 `@chat` JSON，也显示在 CoreS3 屏幕。或者关闭串口监视器，用已有脚本：

  ```bash
  # 在仓库根目录；Python 环境需安装 pyserial
  python3 esp32/tools/muse/chat.py --port /dev/ttyACM0 '你好'
  ```

- **录音**：按住 CoreS3 左侧 PWR 说话，松开上传。录音上限约 15 秒；网络传输在录音结束后进行。没有 ASR 时屏幕会显示“语音识别尚未配置”，开启保存后可在电脑检查 WAV。配置 ASR 和 TTS 后，网关分段处理文字并播报；VoxCPM 可边生成边接收音频，每条回复音频最多 60 秒。
- **补充信息**：nanoMuse 问“你指哪个设备？”等问题时，CoreS3 会显示并播报追问，播完回到待机。再次按住 PWR 回答即可，网关保留原会话，让 nanoMuse 接着处理。等待你回答不会触发网关的模型回复超时；主动取消或设备断线会停止仍在等待的任务，nanoMuse 自身的追问有效期仍然适用。
- **状态**：`THINKING` 表示等待回答；文字已到而语音仍在生成/接收时显示 `PREPARING AUDIO`；只阅读文字时显示 `REPLY`；实际播放时显示 `SPEAKING`；流式音频停顿超过 1 秒时回到等待状态，新音频到达后恢复 `SPEAKING`。有语音时，收到 `audio_end` 且剩余音频播完后恢复 `READY`，无需额外等待 `done` 消息。设备若还有积压录音，会自动发送下一条，再次进入等待状态，串口有 `sending a saved note` 日志。
- **取消**：回复等待中再次按 PWR 可以打断并开始录音；串口 `>chat.cancel` 取消文字请求。取消会请求 nanoMuse 停止当前线程，不会撤销已经执行的外部操作。
- **操作确认**：当前对话需要授权时，屏幕显示可上下滚动的审批卡，包含工具、风险、摘要、目标、原因和服务端提供的参数预览。查看后点击 **允许一次** 或 **拒绝**，nanoMuse 收到决定后继续处理。只授权当前操作，不设置永久权限；超长详情或不支持单次授权的操作只能在设备上拒绝，允许操作需到网页查看处理。
- **审批失效**：在网页处理、取消当前请求、断线或回复结束时，设备审批卡会关闭。重复点击和旧卡片不能授权后续操作。网络发送失败时可重试；等待审批最多额外延长模型等待时间 120 秒。设备仅处理本次设备对话中的前台审批，不接收其他设备、其他会话或后台任务的审批。
- **后台通知**：待机屏幕右上角出现 **通知 N**，点击后可上下滚动查看摘要，用 **上一条 / 下一条** 翻页，**已读** 关闭当前通知，**稍后** 返回待机并保留未读。后台任务结果、失败、新文件以及待审批/待补充信息会进入通知。通知到达会唤醒屏幕，不会自动播报，也不会覆盖录音、思考、播放和当前审批界面；对话结束后可查看。
- **通知范围与保存**：网关默认每 5 秒读取同一 nanoMuse 账户的 Feed，跳过静默结果和仍在运行的过程文字。首次启用不推送历史完成结果；后续断线或重启会恢复未读，并从最近 60 条 Feed 补取新消息。最多保留最新 **6 条**，超出范围和完整内容在网页查看。后台审批/追问只作提醒，需在网页处理；点击“已读”不会批准或回答任务。共用设备密钥的设备共用通知已读状态，仅影响本网关，不修改网页的任务状态。
- **错误排查**：Wi-Fi 页无 IP，先解决联网；网关不能 Connected，检查电脑 IP、设备密钥、TCP 8765 与网络隔离；Connected 但文字报错，检查 nanoMuse 是否运行、server token 及电脑上的模型配置。

后台通知需要更新网关并重新烧录固件，无需增加服务或密钥。现有 `config.toml` 未写 `[notifications]` 时也默认开启；可添加以下配置调整或关闭：

```toml
[notifications]
enabled = true
poll_interval = 5
state_file = ".state/notifications.json"
```

通知缓存只保存在电脑，文件权限为 `0600`，`.state/` 已忽略；其中包含消息摘要，不要提交或分享。网关的 nanoMuse URL/token 改变时，旧缓存不会发给新账户。
USB `>status` 返回的 `device.notifications` 是设备收到的未读数，可用于检查网关到设备的通知连接。

审批验证使用模拟工具，不执行真实的发送、删除或付款操作；真实模型行为和录音音质需要在你的服务配置下确认。

## 5. 配置 ASR 和 TTS

语音服务运行在电脑或局域网里，网关不会自动安装模型。支持 `provider = "openai"` 和 `provider = "blackbox"` 两类接口，ASR 和 TTS 可以分别选择。不要把模型密钥烧进开发板。

### 使用 ASR Blackbox / TTS Blackbox

这两个服务分别使用 `POST /asr-blackbox` 和 `POST /tts-blackbox`，需要明确选择 `provider = "blackbox"`，单独改 URL 不能适配字段差异。完整示例见 [config.blackbox.example.toml](config.blackbox.example.toml)。将其中的语音段落填入本机 `config.toml`，保留已有网关、设备密钥和 nanoMuse 配置：

```toml
[asr]
provider = "blackbox"
url = "http://127.0.0.1:5002/asr-blackbox"
model = "qwen"
language = "Chinese"
key_env = "ASR_API_KEY"

[tts]
provider = "blackbox"
url = "http://127.0.0.1:5050/tts-blackbox"
model = "voxcpmtts"
key_env = "TTS_API_KEY"
```

端口是网关所在电脑实际能访问的端口；网关在宿主机运行时，`127.0.0.1` 指宿主机。ASR 示例选 5002，避免与其他模型的 5001 端口冲突。如果 Blackbox 在另一台机器，请改为它的局域网地址。这两个 Blackbox 默认不验证 API key，`.env` 中的 `ASR_API_KEY`、`TTS_API_KEY` 可以留空；模型后端的配置和凭据仍由各自 `.env.yaml` 管理。

ASR 支持 `qwen`、`sensevoice`、`fun-asr`。语言分别使用 `Chinese`、`zh`/`auto`、`中文`，也可以删除 `language` 使用服务默认值。网关把 WAV 放在 multipart 的 `audio` 字段，模型放在 `model_name`；Qwen/FunASR 读取顶层 `text`，SenseVoice 按顺序合并 `result` 中的 `clean_text`（没有时回退 `text`）。

TTS 支持 `voxcpmtts`、`melotts`、`cosyvoicetts`、`sovitstts`，需要对应后端已启动。网关发送 JSON `{"text":"...","settings":{"model_name":"...",...}}`，强制非流式 WAV 输出，避免不同模型的流格式差异。返回音频仍统一转换为 CoreS3 的 16 kHz PCM。`tts.voice` 仅用于 OpenAI 兼容接口；Blackbox 的音色、参考录音默认沿用它自己的 `.env.yaml`，额外参数可写成：

```toml
[tts.parameters]
# 只填所选模型支持的参数，例如 CosyVoice：
model = "sft"
spk_id = "中文女"
```

如果使用现有 ASR Blackbox 源码及其已安装的虚拟环境，可以在它的目录启动 HTTP 包装服务（模型后端需另外启动）：

```bash
cd /你的路径/asr-blackbox
uv run --no-sync python - <<'PY'
import sys
sys.argv = ["main.py"]
from main import app
import uvicorn
uvicorn.run(app, host="127.0.0.1", port=5002)
PY
```

该项目的 `Configuration` 把 `sys.argv[1]` 当配置文件名，所以此启动方法先清理参数，避免把 `main:app` 误读成文件。若依赖尚未安装，先按该项目自己的说明安装。TTS Blackbox 可继续使用现有 5050 端口。本文按本机提供的 Blackbox README 和处理器源码适配，不改变两个服务的模型安装或后端配置。

### 使用 OpenAI 兼容接口

编辑本机 `config.toml` 的 `[asr]` 和 `[tts]`。下面的本机地址是示例，需要替换为实际运行的服务地址，模型名和音色也必须与服务一致：

```toml
[asr]
provider = "openai"
url = "http://127.0.0.1:8000/v1/audio/transcriptions"
model = "你的识别模型名"
language = "zh"
key_env = "ASR_API_KEY"

[tts]
provider = "openai"
url = "http://127.0.0.1:8001/v1/audio/speech"
model = "你的合成模型名"
voice = "服务支持的音色名"
key_env = "TTS_API_KEY"
# speed = 1.0
```

在 `.env` 中填写密钥（无需认证的本地服务可以留空）：

```dotenv
ASR_API_KEY=你的识别服务密钥
TTS_API_KEY=你的合成服务密钥
```

ASR 按 [transcriptions 接口](https://developers.openai.com/api/reference/resources/audio/subresources/transcriptions/methods/create)发送 multipart：`file`（WAV）、`model`、`response_format=json` 和可选 `language`，接收 `{"text":"识别结果"}`。TTS 按 [speech 接口](https://developers.openai.com/api/reference/resources/audio/subresources/speech/methods/create)发送 JSON：`model`、`voice`、`input`、`response_format=wav`，可选 `speed`，接收 WAV 文件内容。认证均为 `Authorization: Bearer <对应密钥>`。URL 必须是完整接口地址；供应商提供聊天接口并不表示其同时提供这些语音接口。

TTS 需要电脑安装 **ffmpeg**（Ubuntu/Debian：`sudo apt install ffmpeg`），用于把服务返回的 WAV 转成 CoreS3 的 16 kHz、单声道、16-bit PCM。已安装可用 `ffmpeg -version` 检查。不支持只返回 MP3、JSON 下载链接或私有 WebSocket 协议的服务，需另加适配。TTS 请求失败、音频无效或超过 60 秒时，串口输出 `warning`，屏幕保留文字回复。

配置后重启网关：

```bash
uv run --locked --env-file .env gateway.py --config config.toml
```

ASR 配置修改无需重刷固件；**启用 TTS 需先构建并烧录支持接收音频的新固件**，旧固件会继续只显示文字：

```bash
# 在仓库根目录执行；按实际串口替换 /dev/ttyACM0
esp32/tools/nanomuse.sh build
esp32/tools/nanomuse.sh -p /dev/ttyACM0 flash monitor
```

使用前先确认电脑 nanoMuse 页面能正常文字聊天；语音接口不会修复 nanoMuse 的 server token 或模型配置。接通后按住 PWR 说话，松开即可执行“录音 → ASR → nanoMuse → 屏幕文字 → TTS 播报”；播报中再次按 PWR 会停止上一轮并开始新录音。屏幕字幕按播放进度粗略分页，不含逐字时间戳。

## 图片展示

更新网关依赖（`cd nanomuse-gateway && uv sync --locked`），并重新构建、烧录固件。现有 `config.toml` 无需增加配置，默认启用；需要关闭可添加 `[images]`、`enabled = false`。

当本次对话生成或更新工作区中的图片时，网关取最后一张生成缩略预览；回复中的本地 Markdown 图片链接也可显示。例如对 CoreS3 说：

> 请生成一张小猫图片，保存为工作区中的 PNG 文件，并在回复中附上这张图片。

生成图片本身依赖 nanoMuse 已配置的图片模型或绘图工具。若工作区已有图片，可以说：

> 请展示工作区的 pictures/demo.png，用 Markdown 图片链接回复：![预览](/api/files/pictures/demo.png)。

图片下载和解码在电脑端进行，保持比例、黑边补齐到 CoreS3 的 320×240 屏幕。设备收到完整图片后，等语音和字幕结束再全屏展示；点击图片关闭，按住 PWR 开始下一次录音也会退出。阅读设置、审批或通知时延后展示。图片处理在回复完成后进行，不等待图片才发送文字或启动语音。图片失败会输出 warning，保留文字和语音。

支持 PNG、JPEG（含 EXIF 方向）、WebP 和 GIF 首帧；透明部分显示黑色。当前每次回复预览最后一张，尚无相册、缩放、网页截图或后台通知图片预览。源文件限 8 MiB、解码尺寸限 4,194,304 像素；SVG 和外部网址不直接下载，可先让 nanoMuse 将图片保存到工作区。网关只读取 nanoMuse 的 `/api/files/`，不跟随下载重定向；server token 始终留在电脑。旧固件不声明图片能力时不会收到图片数据。

USB `>status` 的 `device.image_visible` 表示图片是否正在显示，`image_received` 表示尚未展示的图片已接收字节。取消、换轮次、断线或图片不完整时，丢弃未展示的图片。

## 协议和验证

`GET /device` 升级 WebSocket，用 `Authorization: Bearer <设备密钥>` 认证。网关先发送 `{"type":"hello","protocol":1}`，支持通知时增加 `notifications:true`。对话控制消息含非零 uint32 `id`；连接级通知消息不含对话 ID：

| 方向 | 消息 |
| --- | --- |
| 设备 → 网关 | `text` + `text`（最多 4000 UTF-8 字节） |
| 设备 → 网关 | `begin` + `rate:16000, channels:1, format:"s16le"`；新固件加 `tts:true` 请求播报 |
| 设备 → 网关 | binary：4 字节 little-endian ID + 最多 2048 字节 PCM |
| 设备 → 网关 | `end` 或 `cancel` |
| 设备 → 网关 | `subscribe` + `notifications:true` 订阅后台通知；`notification_read` + 随机 `token` 标记已读（不含 `id`） |
| 网关 → 设备 | `notifications` + `items` 全量未读快照，每项含 `token`、`kind`、`title`、`text`（不含 `id`，最多 6 项） |
| 设备 → 网关 | `text`/`begin` 加 `approvals:true` 启用设备审批；`approval_decision` 含当前 `id`、卡片 `token` 和布尔值 `approved` |
| 网关 → 设备 | `heard`, `sent`, `reply`, `warning`, `done`, `error`；文字在 `text` |
| 网关 → 设备 | 有真实新进展时发送 `progress` + 当前 `id` 和递增 uint32 `seq`，不改变字幕或播放状态；重复或旧轮次进度不续期 |
| 网关 → 设备 | `approval` 含当前 `id`、随机 `token`、详情 `text`、`can_approve`；`approval_closed` 关闭对应卡片 |
| 网关 → 设备 | 可选 `audio` + `rate:16000, channels:1, format:"s16le", frames:N`，随后 binary（4 字节 little-endian ID + 最多 2048 字节 PCM）和 `audio_end` |
| 设备 → 网关 | `text`/`begin` 加 `images:{"width":320,"height":240}` 声明图片能力和实际屏幕尺寸 |
| 网关 → 设备 | 回复 `done` 后，可选 `image` + 当前 `id`、`width`、`height`、`format:"rgb565be"`、`bytes`，然后图片 binary 和 `image_end` |

图片 binary 头为 4 字节零值、ASCII `IMG1`、4 字节 little-endian 对话 ID、4 字节 little-endian 字节偏移，随后最多 2048 字节 RGB565（高字节在前）。零 ID 保留用于区分图片与 PCM。设备按偏移顺序收齐并收到 `image_end` 后才允许显示；图片传输总期限 15 秒。图片可以在 `audio_end`/`done` 后到达，但对话 ID 必须仍是最近提交的有效轮次。屏幕尺寸必须是整数、单边不超过 640、总像素不超过 153600。

一次连接上的 ID 不重用。旧 ID 的文字和音频在设备端丢弃；超大录音、消息和上传等待均有上限。网关最多同时接入 4 台设备，每台独立线程；回复显示和合成输入限制为 6000 UTF-8 字节，截断时提示在电脑查看全文。网关模型无新进展超时默认 150 秒（`nanomuse.turn_timeout`），整轮最长 900 秒（`nanomuse.max_turn_timeout`，含 ASR、模型及排队的 TTS）。新文字、工具状态或输出变化会刷新无进展计时；重复轮询、时间戳更新和其他会话/后台事件不会续期。新网关最多每 2 秒按真实进展发送 `progress`，CoreS3 收到有效进度序号、新文字或 PCM 后刷新等待；设备无进展上限为 180 秒，审批卡等待另有最多 120 秒余量，整轮上限 1200 秒。流式 TTS 每段总超时仍为 75 秒。播放缓冲在 PSRAM 分配，最多约 1.92 MB；流式回复边接收边播放，终止事件等待音频读完。

```bash
uv run --project nanomuse-gateway --locked python -m unittest discover -s nanomuse-gateway/tests -v
python3 -m unittest discover -s esp32/tests -p test_nanomuse_transport.py -v
# 全部现有 ESP32 主机测试
python3 -m unittest discover -s esp32/tests -p 'test_*.py'
```

网关测试通过模拟 nanoMuse、ASR 和 TTS 验证文字、录音格式、音频转换、最终消息更新、认证、取消、超时和数据上限；固件主机测试运行真实 C 传输代码，验证 WebSocket 文字/二进制分块解析、录音/播放缓冲、旧回复隔离及结束事件。音频转换测试需要 ffmpeg。

网关日志中的 `gateway.latency` 只记录请求编号和耗时，不记录文字、音频或凭据：`first_text` 是 ASR 完成后到首个模型文字片段，`first_audio` 是到首块可播放 PCM，`final_text` 是到完整文字回答。正常流式回答可以看到 `first_audio` 早于 `final_text`；若后端直到回答完成才推送文字，则文字分段无法提前触发。

实现参考：[Pipecat 的文字聚合模式](https://github.com/pipecat-ai/pipecat/blob/main/src/pipecat/services/cartesia/tts.py)、[VoxCPM 的流式生成接口](https://github.com/OpenBMB/VoxCPM)。VoxCPM 当前 HTTP 接口每次接收一段文字；网关用短语请求衔接持续生成的文本，并流式消费每次请求的音频。

流式字幕按语音短语对齐：网关在每段 PCM 前发送短语文字和起始采样位置，CoreS3 按已播放的采样位置切换字幕。后续文字生成、音频下载和最终回复不会推进当前短语的字幕。这是短语级同步，TTS 接口没有提供逐字时间戳。此功能需要更新网关并重新烧录固件；旧固件忽略新字幕消息，仍使用原先的比例估算。

CoreS3 画面冻结诊断：本地配置使用 LVGL 单线程同步绘制，避开异步 `swdraw` 完成等待。USB `>status` 的 `device.ui_frames` 应持续增长；若语音正常但该值不变，表示显示任务停止推进。该配置需要重新烧录固件。

字幕容量协商需要同时更新网关和固件。每个 TTS 短句都有对应的采样位置字幕标记；长句会拆成单屏可显示的小段，避免长字幕被省略或超长标记被设备忽略。

长回答超时修复需要同时重启网关并重新烧录 CoreS3；只更新网关时，旧固件仍受原来的 360 秒总等待限制。语音的 60 秒容量限制独立于超时：达到该限制会提示 warning 并保留文字，不会扩大发送/播放缓冲。日志中的 `model_idle_timeout`、`turn_deadline` 和 `tts_failed` 分别指模型无进展、整轮达到上限和 TTS 失败；`progress` 不代表增加模型生成速度。

自动休眠同时要求设备持续处于 IDLE、且没有按键/触摸，均超过设置的休眠时长。等待回答和播报的时间不计入这段空闲；成功结束或超时回到 IDLE 后重新计算，避免错误提示刚出现就熄屏。设置里的自动休眠时长保持有效。
