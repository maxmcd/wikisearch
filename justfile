# Run the local development server
serve:
    go run cmd/serve/main.go



# Download Wikipedia dump files
download:
    curl -O https://dumps.wikimedia.org/simplewiki/20260101/simplewiki-20260101-pages-articles-multistream.xml.bz2
    curl -O https://dumps.wikimedia.org/simplewiki/20260101/simplewiki-20260101-pages-articles-multistream-index.txt.bz2

# Build the search index from Wikipedia dump
index:
    go run main.go simplewiki-20260101-pages-articles-multistream.xml.bz2

# Deploy to Cloudflare Workers
deploy:
    bun x wrangler deploy
