# Frontend Build Instructions / 前端构建说明 / フロントエンド構築手順

## English

### Frontend Repository

- **Pinned frontend source**: see `../frontend-source.env`

### Build Requirements

1. Run `bash scripts/prepare-frontend.sh` from the Komari repository root
2. The script clones the repo/ref pinned in `../frontend-source.env` unless you override it
3. Ensure the `frontend/dist` folder contains `index.html`

### Important Note

⚠️ **The projects under Akizon77's personal repository are no longer maintained. Please use the projects under the organization (komari-monitor).**

---

## 中文

### 前端项目仓库

- **固定前端来源**: 查看 `../frontend-source.env`

### 构建要求

1. 在 Komari 仓库根目录执行 `bash scripts/prepare-frontend.sh`
2. 脚本默认会克隆 `../frontend-source.env` 中固定的仓库和提交，除非你显式覆盖
3. 确保 `frontend/dist` 文件夹内包含 `index.html`

### 重要提醒

⚠️ **Akizon77 个人仓库的项目已经不再使用，请使用组织（komari-monitor）下的项目。**

---

## 日本語

### フロントエンドプロジェクトリポジトリ

- **固定されたフロントエンドソース**: `../frontend-source.env` を参照

### ビルド要件

1. Komari リポジトリのルートで `bash scripts/prepare-frontend.sh` を実行する
2. スクリプトは `../frontend-source.env` に固定されたリポジトリとコミットを、明示的に上書きしない限り利用する
3. `frontend/dist` フォルダー内に `index.html` が含まれていることを確認する

### 重要な注意事項

⚠️ **Akizon77 の個人リポジトリのプロジェクトは使用されなくなりました。組織（komari-monitor）下のプロジェクトを使用してください。**

---

## Quick Setup / 快速设置 / クイックセットアップ

```bash
# Clone backend repository / 克隆后端仓库 / バックエンドリポジトリをクローン
git clone https://github.com/komari-monitor/komari
cd komari

# Build pinned frontend bundle / 构建固定前端产物 / 固定されたフロントエンド成果物をビルド
bash scripts/prepare-frontend.sh

# Update the pin from a local checkout when needed / 需要时从本地检出更新 pin / 必要に応じてローカルチェックアウトから pin を更新
bash scripts/update-frontend-pin.sh --from-local /path/to/komari-web
```
