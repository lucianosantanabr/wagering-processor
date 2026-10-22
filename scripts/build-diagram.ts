/**
 * Gera o diagrama de arquitetura em DOIS formatos a partir de uma única definição:
 *   docs/architecture.svg     imagem exibida no README
 *   docs/architecture.drawio  fonte editável (abre direto em app.diagrams.net / draw.io)
 *
 *   bun run diagram
 *
 * Se preferir editar no draw.io: ajuste o .drawio e exporte o SVG por cima (Arquivo > Exportar como > SVG);
 * este gerador deixa de ser necessário.
 */
import { mkdirSync, writeFileSync } from 'node:fs';
import { dirname } from 'node:path';

type Pt = [number, number];
interface Line {
  t: string;
  b?: boolean;
  size?: number;
  color?: string;
}
interface Box {
  id: string;
  x: number;
  y: number;
  w: number;
  h: number;
  fill: string;
  stroke: string;
  lines: Line[];
  align?: 'c' | 'l';
  dashed?: boolean;
}
interface Group {
  id: string;
  x: number;
  y: number;
  w: number;
  h: number;
  label: string;
  fill: string;
  stroke: string;
  /** cópias deslocadas atrás do grupo, sugerindo "N instâncias" */
  stacked?: number;
}
interface Edge {
  pts: Pt[];
  label?: { lines: string[]; x: number; y: number; anchor?: 'start' | 'middle' | 'end' };
}

const W = 1300;
const H = 780;
const FONT = 'Helvetica, Arial, sans-serif';
const LH = 15;

const C = {
  app: { fill: '#dae8fc', stroke: '#6c8ebf' },
  worker: { fill: '#fff2cc', stroke: '#d6b656' },
  obs: { fill: '#e1d5e7', stroke: '#9673a6' },
  db: { fill: '#d5e8d4', stroke: '#82b366' },
  queue: { fill: '#ffe6cc', stroke: '#d79b00' },
  dlq: { fill: '#f8cecc', stroke: '#b85450' },
  ext: { fill: '#f5f5f5', stroke: '#666666' },
  note: { fill: '#ffffff', stroke: '#999999' },
};

const t = (text: string, o: Partial<Line> = {}): Line => ({ t: text, ...o });
const b = (text: string, size = 12): Line => ({ t: text, b: true, size });

const groups: Group[] = [
  { id: 'g-app', x: 450, y: 60, w: 480, h: 570, label: 'Aplicação · N instâncias idênticas (HTTP + consumer + workers)', fill: '#f4f8fd', stroke: C.app.stroke, stacked: 2 },
  { id: 'g-db', x: 980, y: 60, w: 300, h: 450, label: 'PostgreSQL · garantias no schema', fill: '#f3f9f2', stroke: C.db.stroke },
];

const boxes: Box[] = [
  { id: 'provider', x: 20, y: 250, w: 150, h: 100, ...C.ext, lines: [b('Provedores de jogos'), t('BET · WIN · LOSS', { size: 11 }), t('REFUND · ROLLBACK', { size: 11 })] },

  { id: 'q-main', x: 220, y: 430, w: 200, h: 60, ...C.queue, lines: [b('wager-transactions.fifo'), t('MessageGroupId = walletId', { size: 11 })] },
  { id: 'q-dlq', x: 220, y: 540, w: 200, h: 60, ...C.dlq, lines: [b('wager-transactions-dlq.fifo', 11), t('mensagens para investigar', { size: 11 })] },

  { id: 'http', x: 470, y: 100, w: 190, h: 70, ...C.app, lines: [b('API HTTP'), t('NestJS + Fastify', { size: 11 }), t('wallets · health · metrics', { size: 11 })] },
  { id: 'consumer', x: 470, y: 190, w: 190, h: 90, ...C.app, lines: [b('Consumer SQS'), t('inbox (consumer, messageId)', { size: 11 }), t('ack só após o COMMIT', { size: 11 }), t('retry com backoff → DLQ', { size: 11 })] },
  {
    id: 'usecase', x: 700, y: 100, w: 210, h: 220, ...C.app, align: 'l',
    lines: [
      b('SubmitWagerTransaction'),
      t('mesmo caso de uso em HTTP e SQS', { size: 11, color: '#555' }),
      t(' '),
      t('1  idempotência (key + hash)', { size: 11 }),
      t('2  lock da wallet (FOR UPDATE)', { size: 11 }),
      t('3  regras de BET, WIN, LOSS,', { size: 11 }),
      t('    REFUND e ROLLBACK', { size: 11 }),
      t('4  saldo, ledger, inbox, outbox', { size: 11 }),
      t('    na MESMA transação SQL', { size: 11 }),
      t('5  ack/resposta só após o COMMIT', { size: 11 }),
    ],
  },
  { id: 'pending', x: 470, y: 370, w: 440, h: 70, ...C.worker, lines: [b('Worker de PENDING_REFERENCE'), t('reaplica as regras quando a referência chega', { size: 11 }), t('backoff 2s…5min · 15 tentativas → REJECTED (REFERENCE_NOT_FOUND)', { size: 11 })] },
  { id: 'publisher', x: 470, y: 460, w: 440, h: 70, ...C.worker, lines: [b('Publisher da outbox (transactional outbox)'), t('FOR UPDATE SKIP LOCKED · backoff · pelo menos uma vez', { size: 11 }), t('lock de linha: vários publishers sem duplicar', { size: 11 })] },
  { id: 'obs', x: 470, y: 550, w: 310, h: 60, ...C.obs, lines: [b('Observabilidade'), t('logs JSON · /metrics · /health/live · /ready', { size: 11 })] },

  { id: 't-wallets', x: 1000, y: 100, w: 260, h: 64, ...C.db, lines: [b('wallets'), t('saldo ≥ 0 · version só sobe com o saldo', { size: 11 })] },
  { id: 't-tx', x: 1000, y: 180, w: 260, h: 64, ...C.db, lines: [b('wager_transactions'), t('UNIQUE(idempotency_key, provider+id)', { size: 11 }), t('estados terminais imutáveis', { size: 11 })] },
  { id: 't-ledger', x: 1000, y: 260, w: 260, h: 64, ...C.db, lines: [b('wallet_ledger_entries'), t('append-only (UPDATE/DELETE bloqueados)', { size: 11 }), t('saldo == ledger verificado no COMMIT', { size: 11 })] },
  { id: 't-inbox', x: 1000, y: 340, w: 260, h: 64, ...C.db, lines: [b('inbox_messages'), t('PK (consumer_name, message_id)', { size: 11 }), t('mesmo id + corpo diferente → DLQ', { size: 11 })] },
  { id: 't-outbox', x: 1000, y: 420, w: 260, h: 64, ...C.db, lines: [b('outbox_messages'), t('published_at · next_attempt_at · seq', { size: 11 })] },

  { id: 'q-events', x: 980, y: 590, w: 300, h: 60, ...C.queue, lines: [b('wager-events.fifo'), t('eventos de integração (envelope versionado)', { size: 11 })] },
  { id: 'downstream', x: 980, y: 690, w: 300, h: 50, ...C.ext, lines: [b('Consumidores dos eventos'), t('deduplicam por eventId', { size: 11 })] },

  { id: 'note', x: 20, y: 640, w: 400, h: 100, ...C.note, dashed: true, align: 'l', lines: [b('Garantias no banco (não só no código)'), t('UNIQUE · CHECK · FKs compostas · triggers', { size: 11 }), t('saldo == ledger no COMMIT (constraint trigger)', { size: 11 }), t('reversão única por referência', { size: 11 })] },
];

const edges: Edge[] = [
  { pts: [[170, 300], [330, 300], [330, 135], [470, 135]], label: { lines: ['HTTP · Idempotency-Key no header', 'resposta só após o COMMIT'], x: 176, y: 276, anchor: 'start' } },
  { pts: [[95, 350], [95, 460], [220, 460]], label: { lines: ['SQS · at-least-once', 'messageId + chave no corpo'], x: 102, y: 400, anchor: 'start' } },
  { pts: [[420, 450], [435, 450], [435, 235], [470, 235]] },
  { pts: [[320, 490], [320, 540]], label: { lines: ['venenosa ou', 'retries esgotados'], x: 328, y: 512, anchor: 'start' } },
  { pts: [[660, 135], [700, 135]] },
  { pts: [[660, 235], [700, 235]] },
  { pts: [[910, 212], [980, 212]], label: { lines: ['1 transação', 'SQL'], x: 945, y: 188, anchor: 'middle' } },
  { pts: [[910, 405], [980, 405]], label: { lines: ['reaplica'], x: 945, y: 397, anchor: 'middle' } },
  { pts: [[910, 495], [980, 495]], label: { lines: ['lê pendentes'], x: 945, y: 487, anchor: 'middle' } },
  { pts: [[870, 530], [870, 620], [980, 620]], label: { lines: ['publica'], x: 878, y: 575, anchor: 'start' } },
  { pts: [[1130, 650], [1130, 690]], label: { lines: ['at-least-once'], x: 1138, y: 674, anchor: 'start' } },
];

// ───────────── proteção: nenhum texto pode estourar a caixa (estimativa conservadora da largura) ─────────────
const estWidth = (s: string, size: number, bold: boolean) => s.length * size * (bold ? 0.58 : 0.54);
function checkFit(): void {
  const errors: string[] = [];
  for (const bx of boxes) {
    const lines = bx.lines;
    for (const l of lines) {
      const w = estWidth(l.t, l.size ?? 11, !!l.b);
      if (w > bx.w - 16) errors.push(`${bx.id}: "${l.t}" ~${Math.round(w)}px > ${bx.w - 16}px`);
    }
    if (lines.length * LH + 12 > bx.h) errors.push(`${bx.id}: ${lines.length} linhas não cabem em h=${bx.h}`);
  }
  for (const g of groups) {
    const w = estWidth(g.label, 12, true);
    if (w > g.w - 20) errors.push(`${g.id}: rótulo ~${Math.round(w)}px > ${g.w - 20}px`);
  }
  if (errors.length) throw new Error(`texto não cabe:\n  ${errors.join('\n  ')}`);
}

// ───────────── SVG ─────────────
const esc = (s: string) => s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');

function svgBox(bx: Box): string {
  const n = bx.lines.length;
  const blockH = n * LH;
  const y0 = bx.y + (bx.h - blockH) / 2 + LH * 0.78;
  const left = bx.align === 'l';
  const tx = left ? bx.x + 10 : bx.x + bx.w / 2;
  const texts = bx.lines
    .map((l, i) => {
      const size = l.size ?? 11;
      return `<text x="${tx}" y="${(y0 + i * LH).toFixed(1)}" text-anchor="${left ? 'start' : 'middle'}" font-size="${size}" ${l.b ? 'font-weight="bold"' : ''} fill="${l.color ?? '#1a1a1a'}" xml:space="preserve">${esc(l.t)}</text>`;
    })
    .join('\n    ');
  return `<g id="${bx.id}">
    <rect x="${bx.x}" y="${bx.y}" width="${bx.w}" height="${bx.h}" rx="7" fill="${bx.fill}" stroke="${bx.stroke}" stroke-width="1.5" ${bx.dashed ? 'stroke-dasharray="6 4"' : ''}/>
    ${texts}
  </g>`;
}

function svgGroup(g: Group): string {
  const ghosts = Array.from({ length: g.stacked ?? 0 }, (_, i) => {
    const o = ((g.stacked ?? 0) - i) * 7;
    return `<rect x="${g.x + o}" y="${g.y + o}" width="${g.w}" height="${g.h}" rx="10" fill="#ffffff" stroke="${g.stroke}" stroke-width="1" opacity="0.55"/>`;
  }).join('\n  ');
  return `<g id="${g.id}">
  ${ghosts}
  <rect x="${g.x}" y="${g.y}" width="${g.w}" height="${g.h}" rx="10" fill="${g.fill}" stroke="${g.stroke}" stroke-width="1.5"/>
  <text x="${g.x + 12}" y="${g.y + 20}" font-size="12" font-weight="bold" fill="#333">${esc(g.label)}</text>
</g>`;
}

function svgEdge(e: Edge, i: number): string {
  const d = e.pts.map((p, k) => `${k === 0 ? 'M' : 'L'}${p[0]},${p[1]}`).join(' ');
  const lab = e.label
    ? e.label.lines
        .map((ln, k) => `<text x="${e.label!.x}" y="${e.label!.y + k * 12}" text-anchor="${e.label!.anchor ?? 'start'}" font-size="10" fill="#444" font-style="italic">${esc(ln)}</text>`)
        .join('\n  ')
    : '';
  return `<g id="edge-${i}">
  <path d="${d}" fill="none" stroke="#333" stroke-width="1.6" marker-end="url(#arrow)"/>
  ${lab}
</g>`;
}

function buildSvg(): string {
  return `<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" width="${W}" height="${H}" viewBox="0 0 ${W} ${H}" font-family="${FONT}" role="img" aria-labelledby="title desc">
  <title id="title">Jungle Wagering Processor: arquitetura</title>
  <desc id="desc">Provedores enviam transações por HTTP e SQS a N instâncias da aplicação, que gravam saldo, ledger, inbox e outbox na mesma transação PostgreSQL e publicam eventos por uma outbox transacional.</desc>
  <defs>
    <marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse">
      <path d="M0,0 L10,5 L0,10 z" fill="#333"/>
    </marker>
  </defs>
  <rect width="${W}" height="${H}" fill="#ffffff"/>
  <text x="20" y="34" font-size="20" font-weight="bold" fill="#1a1a1a">Jungle Wagering Processor · visão geral da arquitetura</text>
  <text x="20" y="54" font-size="12" fill="#555">entrega at-least-once · idempotência persistente · saldo materializado == ledger · correto com N instâncias</text>
  ${groups.map(svgGroup).join('\n  ')}
  ${edges.map(svgEdge).join('\n  ')}
  ${boxes.map(svgBox).join('\n  ')}
</svg>
`;
}

// ───────────── draw.io (mxGraph XML, não comprimido) ─────────────
const xa = (s: string) => esc(s);

function drawioBoxValue(bx: Box): string {
  return bx.lines
    .map((l) => {
      const inner = esc(l.t).replace(/ /g, '&#160;');
      const styled = l.b ? `<b>${inner}</b>` : l.color ? `<font color="${l.color}">${inner}</font>` : inner;
      return l.size && !l.b ? `<font style="font-size:${l.size}px">${styled}</font>` : styled;
    })
    .join('<br>');
}

function buildDrawio(): string {
  let id = 2;
  const cells: string[] = [];
  for (const g of groups) {
    for (let i = 0; i < (g.stacked ?? 0); i++) {
      const o = ((g.stacked ?? 0) - i) * 7;
      cells.push(`<mxCell id="${id++}" value="" style="rounded=1;whiteSpace=wrap;html=1;fillColor=#ffffff;strokeColor=${g.stroke};opacity=55;arcSize=3;" vertex="1" parent="1"><mxGeometry x="${g.x + o}" y="${g.y + o}" width="${g.w}" height="${g.h}" as="geometry"/></mxCell>`);
    }
    cells.push(`<mxCell id="${id++}" value="${xa(g.label)}" style="rounded=1;whiteSpace=wrap;html=1;fillColor=${g.fill};strokeColor=${g.stroke};verticalAlign=top;align=left;spacingLeft=12;spacingTop=2;fontStyle=1;fontSize=12;arcSize=3;" vertex="1" parent="1"><mxGeometry x="${g.x}" y="${g.y}" width="${g.w}" height="${g.h}" as="geometry"/></mxCell>`);
  }
  for (const e of edges) {
    const first = e.pts[0]!;
    const last = e.pts[e.pts.length - 1]!;
    const mid = e.pts.slice(1, -1);
    cells.push(`<mxCell id="${id++}" value="" style="endArrow=block;endFill=1;html=1;rounded=0;strokeColor=#333333;strokeWidth=1.6;" edge="1" parent="1"><mxGeometry relative="1" as="geometry"><mxPoint x="${first[0]}" y="${first[1]}" as="sourcePoint"/><mxPoint x="${last[0]}" y="${last[1]}" as="targetPoint"/>${mid.length ? `<Array as="points">${mid.map((p) => `<mxPoint x="${p[0]}" y="${p[1]}"/>`).join('')}</Array>` : ''}</mxGeometry></mxCell>`);
    if (e.label) {
      const lw = 130;
      const anchor = e.label.anchor ?? 'start';
      const x = anchor === 'middle' ? e.label.x - lw / 2 : anchor === 'end' ? e.label.x - lw : e.label.x - 2;
      const al = anchor === 'middle' ? 'center' : anchor === 'end' ? 'right' : 'left';
      cells.push(`<mxCell id="${id++}" value="${xa(e.label.lines.join('<br>')).replace(/&lt;br&gt;/g, '&lt;br&gt;')}" style="text;html=1;fontSize=10;fontStyle=2;fontColor=#444444;align=${al};verticalAlign=top;whiteSpace=wrap;" vertex="1" parent="1"><mxGeometry x="${x}" y="${e.label.y - 10}" width="${lw}" height="${e.label.lines.length * 12 + 4}" as="geometry"/></mxCell>`);
    }
  }
  for (const bx of boxes) {
    const left = bx.align === 'l';
    cells.push(`<mxCell id="${id++}" value="${xa(drawioBoxValue(bx))}" style="rounded=1;whiteSpace=wrap;html=1;fillColor=${bx.fill};strokeColor=${bx.stroke};fontSize=11;align=${left ? 'left' : 'center'};verticalAlign=middle;${left ? 'spacingLeft=10;' : ''}${bx.dashed ? 'dashed=1;' : ''}arcSize=8;" vertex="1" parent="1"><mxGeometry x="${bx.x}" y="${bx.y}" width="${bx.w}" height="${bx.h}" as="geometry"/></mxCell>`);
  }
  cells.push(`<mxCell id="${id++}" value="${xa('<b style="font-size:20px">Jungle Wagering Processor · visão geral da arquitetura</b>')}" style="text;html=1;align=left;verticalAlign=middle;" vertex="1" parent="1"><mxGeometry x="20" y="12" width="900" height="30" as="geometry"/></mxCell>`);
  cells.push(`<mxCell id="${id++}" value="${xa('entrega at-least-once · idempotência persistente · saldo materializado == ledger · correto com N instâncias')}" style="text;html=1;fontSize=12;fontColor=#555555;align=left;verticalAlign=middle;" vertex="1" parent="1"><mxGeometry x="20" y="42" width="900" height="20" as="geometry"/></mxCell>`);

  return `<?xml version="1.0" encoding="UTF-8"?>
<mxfile host="app.diagrams.net" agent="build-diagram.ts" version="24.0.0">
  <diagram id="architecture" name="Arquitetura">
    <mxGraphModel dx="${W}" dy="${H}" grid="1" gridSize="10" guides="1" tooltips="1" connect="1" arrows="1" fold="1" page="1" pageScale="1" pageWidth="${W}" pageHeight="${H}" math="0" shadow="0">
      <root>
        <mxCell id="0"/>
        <mxCell id="1" parent="0"/>
        ${cells.join('\n        ')}
      </root>
    </mxGraphModel>
  </diagram>
</mxfile>
`;
}

checkFit();
for (const [path, content] of [
  ['docs/architecture.svg', buildSvg()],
  ['docs/architecture.drawio', buildDrawio()],
] as const) {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, content, 'utf8');
  console.log(`escrito ${path} (${content.length} bytes)`);
}
