import path from 'node:path';
import readline from 'node:readline';
import { createHash } from 'node:crypto';
import { writeFile } from 'node:fs/promises';
import { ensureDir, pathExists, readJson, readJsonLines, writeJson } from './fs.js';
import { bookmarkMediaDir, twitterBookmarksCachePath } from './paths.js';
import { listBookmarks, countBookmarks } from './bookmarks-db.js';
import type { BookmarkRecord } from './types.js';

export interface MediaFetchEntry {
  bookmarkId: string;
  tweetId: string;
  tweetUrl: string;
  authorHandle?: string;
  authorName?: string;
  sourceUrl: string;
  localPath?: string;
  contentType?: string;
  bytes?: number;
  status: 'downloaded' | 'skipped_too_large' | 'failed';
  reason?: string;
  fetchedAt: string;
}

export interface MediaFetchManifest {
  schemaVersion: 1;
  generatedAt: string;
  limit: number;
  maxBytes: number;
  processed: number;
  downloaded: number;
  skippedTooLarge: number;
  failed: number;
  entries: MediaFetchEntry[];
}

export interface MediaFetchOptions {
  limit?: number;
  maxBytes?: number;
  author?: string;
  category?: string;
  domain?: string;
  query?: string;
  after?: string;
  before?: string;
  includeProfile?: boolean;
  output?: string;
}

function sanitizeExtFromContentType(contentType?: string, sourceUrl?: string): string {
  if (contentType?.includes('jpeg')) return '.jpg';
  if (contentType?.includes('png')) return '.png';
  if (contentType?.includes('gif')) return '.gif';
  if (contentType?.includes('webp')) return '.webp';
  if (contentType?.includes('mp4')) return '.mp4';
  try {
    const ext = path.extname(new URL(sourceUrl ?? '').pathname);
    if (ext) return ext;
  } catch {}
  return '.bin';
}

function slugify(text: string): string {
  return text
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-|-$/g, '')
    .slice(0, 50);
}

export function suggestOutputFolder(options: MediaFetchOptions): string {
  if (options.author) return options.author.replace(/^@/, '');
  if (options.category) return options.category;
  if (options.domain) return options.domain;
  if (options.query) return slugify(options.query);
  return 'all';
}

function ask(prompt: string): Promise<string> {
  return new Promise((resolve) => {
    const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
    let answered = false;
    rl.question(prompt, (ans) => {
      answered = true;
      rl.close();
      resolve(ans.trim());
    });
    rl.on('close', () => {
      if (!answered) resolve('');
    });
  });
}

function manifestPathForFolder(folder: string): string {
  return path.join(bookmarkMediaDir(), folder, '.media-manifest.json');
}

async function loadManifest(folder: string): Promise<MediaFetchManifest | null> {
  const mp = manifestPathForFolder(folder);
  if (!(await pathExists(mp))) return null;
  return readJson<MediaFetchManifest>(mp);
}

async function promptForFolder(suggested: string, totalWithMedia: number): Promise<string> {
  const mediaBase = bookmarkMediaDir();

  console.log();
  console.log(`  ${totalWithMedia} bookmarks with media found. Save to:\n`);
  console.log(`  \x1b[1m1)\x1b[0m  media/${suggested}/\x1b[2m  (recommended)\x1b[0m`);
  console.log(`  \x1b[1m2)\x1b[0m  media/\x1b[2m  (flat, default location)\x1b[0m`);
  console.log(`  \x1b[1m3)\x1b[0m  custom path`);
  console.log();

  const answer = await ask('  Choose [1/2/3] or press enter for 1: ');

  if (!answer || answer === '1') return suggested;
  if (answer === '2') return '';

  if (answer === '3') {
    const custom = await ask(`  Folder name (inside ${mediaBase}/): `);
    return custom || suggested;
  }

  // If they typed a folder name directly, use it
  return answer;
}

export async function fetchBookmarkMediaBatch(
  options: MediaFetchOptions = {}
): Promise<MediaFetchManifest> {
  const limit = options.limit ?? 100;
  const maxBytes = options.maxBytes ?? 50 * 1024 * 1024;
  const includeProfile = options.includeProfile ?? true;

  // Use DB filtering to find matching bookmarks
  const hasFilters = !!(options.author || options.category || options.domain || options.query || options.after || options.before);
  const dbResults = await listBookmarks({
    author: options.author?.replace(/^@/, ''),
    category: options.category,
    domain: options.domain,
    query: options.query,
    after: options.after,
    before: options.before,
    limit: limit,
  });

  const matchingIds = new Set(dbResults.map((r) => r.id));
  const totalWithMedia = dbResults.filter((r) => r.mediaCount > 0).length;

  // Determine output folder
  const suggested = suggestOutputFolder(options);
  let folder: string;
  if (options.output !== undefined) {
    folder = options.output;
  } else {
    folder = await promptForFolder(suggested, totalWithMedia);
  }

  const mediaDir = folder ? path.join(bookmarkMediaDir(), folder) : bookmarkMediaDir();
  await ensureDir(mediaDir);

  // Load JSONL to get media URLs for matching bookmarks
  const allBookmarks = await readJsonLines<BookmarkRecord>(twitterBookmarksCachePath());
  const bookmarkMap = new Map<string, BookmarkRecord>();
  for (const b of allBookmarks) {
    if (matchingIds.has(b.id)) bookmarkMap.set(b.id, b);
  }

  const candidates = dbResults
    .filter((r) => bookmarkMap.has(r.id))
    .map((r) => bookmarkMap.get(r.id)!)
    .filter((b) => (b.media?.length ?? 0) > 0 || (b.mediaObjects?.length ?? 0) > 0 || (includeProfile && b.authorProfileImageUrl));

  const previous = await loadManifest(folder);
  const priorKeys = new Set((previous?.entries ?? []).map((e) => `${e.bookmarkId}::${e.sourceUrl}`));
  const entries: MediaFetchEntry[] = previous?.entries ? [...previous.entries] : [];

  let downloaded = 0;
  let skippedTooLarge = 0;
  let failed = 0;
  let processed = 0;

  for (const bookmark of candidates) {
    // Resolve media URLs: prefer mediaObjects (richer, includes video variants), fall back to media[]
    const mediaUrls: string[] = [];
    if (bookmark.mediaObjects?.length) {
      for (const mo of bookmark.mediaObjects) {
        if (mo.type === 'video' || mo.type === 'animated_gif') {
          const mp4s = (mo.variants ?? [])
            .filter((v) => v.contentType === 'video/mp4' && v.url)
            .sort((a, b) => (b.bitrate ?? 0) - (a.bitrate ?? 0));
          if (mp4s.length > 0 && mp4s[0].url) { mediaUrls.push(mp4s[0].url); continue; }
        }
        const mediaUrl = mo.mediaUrl ?? (mo as any).url;
        if (mediaUrl) mediaUrls.push(mediaUrl);
      }
    } else {
      mediaUrls.push(...(bookmark.media ?? []));
    }

    // Also include author profile image (upgraded to 400x400)
    if (includeProfile && bookmark.authorProfileImageUrl) {
      const fullUrl = bookmark.authorProfileImageUrl.replace('_normal.', '_400x400.');
      if (!priorKeys.has(`${bookmark.id}::${fullUrl}`)) mediaUrls.push(fullUrl);
    }

    for (const sourceUrl of mediaUrls) {
      const key = `${bookmark.id}::${sourceUrl}`;
      if (priorKeys.has(key)) continue;
      processed += 1;

      const fetchedAt = new Date().toISOString();

      try {
        const head = await fetch(sourceUrl, { method: 'HEAD' });
        const contentLengthHeader = head.headers.get('content-length');
        const contentType = head.headers.get('content-type') ?? undefined;
        const declaredBytes = contentLengthHeader ? Number(contentLengthHeader) : undefined;

        if (typeof declaredBytes === 'number' && !Number.isNaN(declaredBytes) && declaredBytes > maxBytes) {
          entries.push({
            bookmarkId: bookmark.id,
            tweetId: bookmark.tweetId,
            tweetUrl: bookmark.url,
            authorHandle: bookmark.authorHandle,
            authorName: bookmark.authorName,
            sourceUrl,
            contentType,
            bytes: declaredBytes,
            status: 'skipped_too_large',
            reason: `content-length ${declaredBytes} exceeds max ${maxBytes}`,
            fetchedAt,
          });
          skippedTooLarge += 1;
          continue;
        }

        const response = await fetch(sourceUrl);
        if (!response.ok) {
          entries.push({
            bookmarkId: bookmark.id,
            tweetId: bookmark.tweetId,
            tweetUrl: bookmark.url,
            authorHandle: bookmark.authorHandle,
            authorName: bookmark.authorName,
            sourceUrl,
            status: 'failed',
            reason: `HTTP ${response.status}`,
            fetchedAt,
          });
          failed += 1;
          continue;
        }

        const buffer = Buffer.from(await response.arrayBuffer());
        if (buffer.byteLength > maxBytes) {
          entries.push({
            bookmarkId: bookmark.id,
            tweetId: bookmark.tweetId,
            tweetUrl: bookmark.url,
            authorHandle: bookmark.authorHandle,
            authorName: bookmark.authorName,
            sourceUrl,
            contentType: response.headers.get('content-type') ?? contentType ?? undefined,
            bytes: buffer.byteLength,
            status: 'skipped_too_large',
            reason: `downloaded size ${buffer.byteLength} exceeds max ${maxBytes}`,
            fetchedAt,
          });
          skippedTooLarge += 1;
          continue;
        }

        const digest = createHash('sha256').update(buffer).digest('hex').slice(0, 16);
        const ext = sanitizeExtFromContentType(response.headers.get('content-type') ?? contentType ?? undefined, sourceUrl);
        const filename = `${bookmark.tweetId}-${digest}${ext}`;
        const localPath = path.join(mediaDir, filename);
        await writeFile(localPath, buffer);

        entries.push({
          bookmarkId: bookmark.id,
          tweetId: bookmark.tweetId,
          tweetUrl: bookmark.url,
          authorHandle: bookmark.authorHandle,
          authorName: bookmark.authorName,
          sourceUrl,
          localPath,
          contentType: response.headers.get('content-type') ?? contentType ?? undefined,
          bytes: buffer.byteLength,
          status: 'downloaded',
          fetchedAt,
        });
        downloaded += 1;
      } catch (error) {
        entries.push({
          bookmarkId: bookmark.id,
          tweetId: bookmark.tweetId,
          tweetUrl: bookmark.url,
          authorHandle: bookmark.authorHandle,
          authorName: bookmark.authorName,
          sourceUrl,
          status: 'failed',
          reason: error instanceof Error ? error.message : String(error),
          fetchedAt,
        });
        failed += 1;
      }
    }
  }

  const manifest: MediaFetchManifest = {
    schemaVersion: 1,
    generatedAt: new Date().toISOString(),
    limit,
    maxBytes,
    processed,
    downloaded,
    skippedTooLarge,
    failed,
    entries,
  };

  const manifestPath = folder ? manifestPathForFolder(folder) : path.join(bookmarkMediaDir(), 'media-manifest.json');
  await writeJson(manifestPath, manifest);

  if (folder) {
    console.log(`\n  \u2713 ${downloaded} files downloaded to media/${folder}/`);
  }

  return manifest;
}
