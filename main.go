package main

import (
	"bufio"
	"compress/bzip2"
	"encoding/binary"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"regexp"
	"sort"
	"strings"
	"unicode"
)

type Posting struct {
	DocID     uint32
	Positions []uint32
}

type Meta struct {
	DocCount      int `json:"docCount"`
	ShardCount    int `json:"shardCount"`
	DocShardCount int `json:"docShardCount"`
}

type Doc struct {
	ID      uint32
	Title   string
	Content string
}

type Page struct {
	Title string `xml:"title"`
	ID    uint32 `xml:"id"`
	NS    int    `xml:"ns"`
	Text  string `xml:"revision>text"`
}

var (
	commentRe       = regexp.MustCompile(`(?s)<!--.*?-->`)
	refRe           = regexp.MustCompile(`(?s)<ref[^>]*>.*?</ref>|<ref[^/]*/>`)
	templateRe      = regexp.MustCompile(`(?s)\{\{[^{}]*\}\}`)
	tableRowRe      = regexp.MustCompile(`(?m)^[^\S\n]*[|!].*$`)
	orphanBraceRe   = regexp.MustCompile(`(?m)^\{\{[A-Za-z][^{}\n]*$`)
	orphanBracketRe = regexp.MustCompile(`(?m)^[^\[\]]*\]\]$`)
	fileRe          = regexp.MustCompile(`(?i)\[\[(File|Image):[^\n]*\]\]`)
	categoryRe      = regexp.MustCompile(`(?i)\[\[Category:[^\]]*\]\]`)
	extLinkRe       = regexp.MustCompile(`\[https?://[^\]]*\]`)
	thumbLineRe     = regexp.MustCompile(`(?im)^(thumb|thumbnail|right|left|center|\d+px)[|].*$`)
	linkRe          = regexp.MustCompile(`\[\[(?:[^|\]]*\|)?([^\]]*)\]\]`)
	tagRe           = regexp.MustCompile(`<[^>]+>`)
	styleRe         = regexp.MustCompile(`'{2,}`)
	headerRe        = regexp.MustCompile(`={2,}\s*([^=]+?)\s*={2,}`)
	multiSpaceRe    = regexp.MustCompile(`[ \t]{2,}`)
	multiNewlineRe  = regexp.MustCompile(`\n{3,}`)
)

func stripWikitext(s string) string {
	s = commentRe.ReplaceAllString(s, "")
	s = refRe.ReplaceAllString(s, "")
	for i := 0; i < 10; i++ {
		prev := s
		s = templateRe.ReplaceAllString(s, "")
		if s == prev {
			break
		}
	}
	s = tableRowRe.ReplaceAllString(s, "")
	s = orphanBraceRe.ReplaceAllString(s, "")
	s = orphanBracketRe.ReplaceAllString(s, "")
	s = fileRe.ReplaceAllString(s, "")
	s = categoryRe.ReplaceAllString(s, "")
	s = extLinkRe.ReplaceAllString(s, "")
	s = thumbLineRe.ReplaceAllString(s, "")
	s = linkRe.ReplaceAllString(s, "$1")
	s = tagRe.ReplaceAllString(s, "")
	s = styleRe.ReplaceAllString(s, "")
	s = headerRe.ReplaceAllString(s, "\n$1\n")
	s = multiSpaceRe.ReplaceAllString(s, " ")
	s = multiNewlineRe.ReplaceAllString(s, "\n\n")
	return strings.TrimSpace(s)
}

func tokenize(s string) []string {
	var tokens []string
	var buf strings.Builder
	for _, r := range strings.ToLower(s) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			buf.WriteRune(r)
		} else if buf.Len() > 0 {
			tokens = append(tokens, buf.String())
			buf.Reset()
		}
	}
	if buf.Len() > 0 {
		tokens = append(tokens, buf.String())
	}
	return tokens
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func writeVarint(w io.Writer, v uint32) {
	var buf [5]byte
	n := binary.PutUvarint(buf[:], uint64(v))
	w.Write(buf[:n])
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("usage: wiki <dump.xml.bz2>")
		os.Exit(1)
	}
	buildIndex(os.Args[1])
}

const shardCount = 4096
const docShardCount = 65536

func writeDocShards(docShards [][]Doc) {
	for shardNum, docs := range docShards {
		if len(docs) == 0 {
			continue
		}
		sf, _ := os.Create(fmt.Sprintf("public/docs/shard_%05d.bin", shardNum))
		bw := bufio.NewWriter(sf)

		binary.Write(bw, binary.LittleEndian, uint32(len(docs)))
		for _, doc := range docs {
			binary.Write(bw, binary.LittleEndian, doc.ID)
			binary.Write(bw, binary.LittleEndian, uint16(len(doc.Title)))
			bw.WriteString(doc.Title)
			binary.Write(bw, binary.LittleEndian, uint32(len(doc.Content)))
			bw.WriteString(doc.Content)
		}

		bw.Flush()
		sf.Close()
	}
}

func buildIndex(dumpPath string) {
	os.RemoveAll("public/docs")
	os.RemoveAll("public/index")
	os.MkdirAll("public/docs", 0755)
	os.MkdirAll("public/index", 0755)

	f, _ := os.Open(dumpPath)
	defer f.Close()

	shards := make([]map[string][]Posting, shardCount)
	for i := range shards {
		shards[i] = make(map[string][]Posting)
	}
	docShards := make([][]Doc, docShardCount)
	docCount := 0

	decoder := xml.NewDecoder(bzip2.NewReader(f))
	for {
		tok, err := decoder.Token()
		if err != nil {
			break
		}
		se, ok := tok.(xml.StartElement)
		if !ok || se.Name.Local != "page" {
			continue
		}
		var page Page
		decoder.DecodeElement(&page, &se)
		if page.NS != 0 {
			continue
		}

		text := stripWikitext(page.Text)
		docShard := page.ID % docShardCount
		docShards[docShard] = append(docShards[docShard], Doc{ID: page.ID, Title: page.Title, Content: text})

		tokens := tokenize(text)
		positions := make(map[string][]uint32)
		for i, t := range tokens {
			positions[t] = append(positions[t], uint32(i))
		}
		for t, pos := range positions {
			shard := hash(t) % shardCount
			shards[shard][t] = append(shards[shard][t], Posting{DocID: page.ID, Positions: pos})
		}

		docCount++
		if docCount%10000 == 0 {
			fmt.Printf("indexed %d docs\n", docCount)
		}
	}

	fmt.Printf("total: %d docs, writing doc shards...\n", docCount)
	writeDocShards(docShards)

	fmt.Printf("total: %d docs, writing shards...\n", docCount)

	for i, shard := range shards {
		sf, _ := os.Create(fmt.Sprintf("public/index/shard_%04d.bin", i))
		bw := bufio.NewWriter(sf)

		tokens := make([]string, 0, len(shard))
		for t := range shard {
			tokens = append(tokens, t)
		}
		sort.Strings(tokens)

		for _, t := range tokens {
			postings := shard[t]
			bw.WriteByte(byte(len(t)))
			bw.WriteString(t)
			binary.Write(bw, binary.LittleEndian, uint32(len(postings)))

			sort.Slice(postings, func(a, b int) bool { return postings[a].DocID < postings[b].DocID })
			var prevDoc uint32
			for _, p := range postings {
				writeVarint(bw, p.DocID-prevDoc)
				prevDoc = p.DocID
				writeVarint(bw, uint32(len(p.Positions)))
				var prevPos uint32
				for _, pos := range p.Positions {
					writeVarint(bw, pos-prevPos)
					prevPos = pos
				}
			}
		}
		bw.Flush()
		sf.Close()
	}

	mf, _ := os.Create("public/index/meta.json")
	json.NewEncoder(mf).Encode(Meta{DocCount: docCount, ShardCount: shardCount, DocShardCount: docShardCount})
	mf.Close()
	fmt.Println("done")
}
