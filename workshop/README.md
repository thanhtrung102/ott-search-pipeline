# Workshop — Local Development

## Install Hugo and the theme

```bash
# Install Hugo (extended version required for SCSS)
# Windows (via scoop):
scoop install hugo-extended

# macOS:
brew install hugo

# Linux:
wget https://github.com/gohugoio/hugo/releases/download/v0.121.0/hugo_extended_0.121.0_linux-amd64.tar.gz
tar -xf hugo_extended_*.tar.gz && sudo mv hugo /usr/local/bin/

# Verify
hugo version
```

## Install the Learn theme

```bash
cd workshop/
mkdir -p themes
git submodule add https://github.com/matcornic/hugo-theme-learn themes/hugo-theme-learn
# Or without submodules:
git clone https://github.com/matcornic/hugo-theme-learn themes/hugo-theme-learn
```

## Run locally

```bash
cd workshop/
hugo server --buildDrafts --port 1313
# Open: http://localhost:1313
```

## Build for production

```bash
hugo --minify
# Output in: workshop/public/
```

## Deploy to FCJ

Copy the `public/` directory contents to your `fcjuni.com` hosting location, or configure in `config.toml`:

```toml
baseURL = "https://your-project.fcjuni.com/"
```
