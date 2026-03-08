import { defineConfig } from 'vite';

export default defineConfig({
  publicDir: 'public',
  build: {
    outDir: 'dist',
  },
  server: {
    port: 5173,
    open: true,
    fs: {
      allow: ['..'],
    },
  },
  plugins: [
    {
      name: 'local-dev-api',
      configureServer(server) {
        // Only runs in dev mode — not included in Vercel build
        import('path').then(pathMod => {
          import('fs').then(fsMod => {
            const path = pathMod.default;
            const fs = fsMod.default;
            const papersDir = path.resolve(process.cwd(), '..', 'meta', '03-papers');
            const reviewDir = path.resolve(process.cwd(), '..', 'meta', '08-human-review');
            const reviewCsvPath = path.join(reviewDir, 'review-queue.csv');

            // Serve PDFs
            server.middlewares.use('/papers', (req, res, next) => {
              const filePath = path.join(papersDir, decodeURIComponent(req.url));
              if (fs.existsSync(filePath) && fs.statSync(filePath).isFile()) {
                const ext = path.extname(filePath).toLowerCase();
                const mimeTypes = {
                  '.pdf': 'application/pdf',
                  '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.png': 'image/png',
                  '.md': 'text/markdown; charset=utf-8',
                  '.csv': 'text/csv; charset=utf-8',
                  '.json': 'application/json; charset=utf-8',
                };
                res.setHeader('Content-Type', mimeTypes[ext] || 'application/octet-stream');
                fs.createReadStream(filePath).pipe(res);
              } else {
                res.statusCode = 404;
                res.end('Not found');
              }
            });

            // PULL
            server.middlewares.use('/api/pull', (req, res) => {
              res.setHeader('Content-Type', 'application/json');
              try {
                if (!fs.existsSync(reviewCsvPath)) {
                  res.statusCode = 404;
                  res.end(JSON.stringify({ error: 'review-queue.csv not found' }));
                  return;
                }
                const csv = fs.readFileSync(reviewCsvPath, 'utf-8');
                res.end(JSON.stringify({ ok: true, csv, path: reviewCsvPath }));
              } catch (err) {
                res.statusCode = 500;
                res.end(JSON.stringify({ error: err.message }));
              }
            });

            // PUSH
            server.middlewares.use('/api/push', (req, res) => {
              if (req.method !== 'POST') {
                res.statusCode = 405;
                res.end(JSON.stringify({ error: 'POST required' }));
                return;
              }
              let body = '';
              req.on('data', chunk => { body += chunk; });
              req.on('end', () => {
                res.setHeader('Content-Type', 'application/json');
                try {
                  const { csv } = JSON.parse(body);
                  if (!csv) {
                    res.statusCode = 400;
                    res.end(JSON.stringify({ error: 'Missing csv field' }));
                    return;
                  }
                  if (fs.existsSync(reviewCsvPath)) {
                    const backupPath = reviewCsvPath.replace('.csv', `_backup_${Date.now()}.csv`);
                    fs.copyFileSync(reviewCsvPath, backupPath);
                  }
                  fs.writeFileSync(reviewCsvPath, csv, 'utf-8');
                  const publicCopy = path.resolve(process.cwd(), 'public', 'data', 'review-queue.csv');
                  if (fs.existsSync(path.dirname(publicCopy))) {
                    fs.writeFileSync(publicCopy, csv, 'utf-8');
                  }
                  res.end(JSON.stringify({ ok: true, path: reviewCsvPath, timestamp: new Date().toISOString() }));
                } catch (err) {
                  res.statusCode = 500;
                  res.end(JSON.stringify({ error: err.message }));
                }
              });
            });

            // Study files
            server.middlewares.use('/api/study-files', (req, res) => {
              const url = new URL(req.url, 'http://localhost');
              const studyId = url.searchParams.get('id');
              if (!studyId) { res.statusCode = 400; res.end('{"error":"Missing id"}'); return; }
              const studyDir = path.join(papersDir, studyId);
              if (!fs.existsSync(studyDir)) { res.statusCode = 404; res.end('{"error":"Not found"}'); return; }
              const files = {
                hasPdf: fs.existsSync(path.join(studyDir, 'original.pdf')),
                hasMarkdown: fs.existsSync(path.join(studyDir, 'extracted-text.md')),
              };
              res.setHeader('Content-Type', 'application/json');
              res.end(JSON.stringify(files));
            });
          });
        });
      },
    },
  ],
});
