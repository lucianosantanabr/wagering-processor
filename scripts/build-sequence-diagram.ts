/**
 * Diagrama de SEQUÊNCIA: um REFUND que chega antes da BET que ele referencia (PENDING_REFERENCE).
 * Gera, de uma única definição:
 *   docs/sequence-pending-reference.svg      imagem exibida no ARCHITECTURE.md
 *   docs/sequence-pending-reference.drawio   fonte editável (app.diagrams.net)
 *
 *   bun run diagram     (gera este e o diagrama de arquitetura)
 */
import { mkdirSync, writeFileSync } from 'node:fs';
import { dirname } from 'node:path';

const W = 1200;
const H = 960;
const FONT = 'Helvetica, Arial, sans-serif';

// ── participantes (linhas de vida) ──
const X = { provider: 150, app: 450, db: 800, worker: 1060 } as const;
type Who = keyof typeof X;

interface Participant {
  id: Who;
  title: string[];
  fill: string;
  stroke: string;
}
const participants: Participant[] = [
  { id: 'provider', title: ['Provedor'], fill: '#f5f5f5', stroke: '#666666' },
  { id: 'app', title: ['Aplicação', 'API HTTP / consumer SQS'], fill: '#dae8fc', stroke: '#6c8ebf' },
  { id: 'db', title: ['PostgreSQL'], fill: '#d5e8d4', stroke: '#82b366' },
  { id: 'worker', title: ['Worker de PENDING_REFERENCE'], fill: '#fff2cc', stroke: '#d6b656' },
];

const HEADER_Y = 66;
const HEADER_H = 50;
const LIFE_TOP = HEADER_Y + HEADER_H;
const LIFE_BOTTOM = H - 20;

// ── fases (faixas de fundo) ──
interface Band {
  y: number;
  h: number;
  label: string;
  fill: string;
  stroke: string;
}
const bands: Band[] = [
  { y: 130, h: 285, label: 'FASE 1 · t = 0 s · o estorno chega ANTES da aposta', fill: '#fdf1ef', stroke: '#e6b8b2' },
  { y: 425, h: 165, label: 'FASE 2 · t ≈ 1 s · a aposta chega', fill: '#eef4fb', stroke: '#b5c9e6' },
  { y: 600, h: 295, label: 'FASE 3 · t ≥ 2 s · o worker reaplica o estorno', fill: '#eef7ec', stroke: '#b7d6b0' },
];

// ── mensagens ──
interface Msg {
  from: Who;
  to: Who;
  y: number;
  lines: string[];
  dashed?: boolean;
}
const msgs: Msg[] = [
  { from: 'provider', to: 'app', y: 200, lines: ['1) REFUND 25,00 (referência = bet-1)', 'por HTTP ou por SQS'] },
  { from: 'app', to: 'db', y: 248, lines: ['2) busca da idempotency key', 'e lock da wallet (FOR UPDATE)'] },
  { from: 'app', to: 'db', y: 296, lines: ['3) busca (provedor, bet-1): NÃO existe'] },
  { from: 'app', to: 'db', y: 350, lines: ['4) INSERT PENDING_REFERENCE (retry em +2 s)', '+ outbox · COMMIT'] },
  { from: 'app', to: 'provider', y: 395, lines: ['5) 202 PENDING_REFERENCE (ou ack da mensagem)'], dashed: true },

  { from: 'provider', to: 'app', y: 478, lines: ['6) BET 25,00 (bet-1)'] },
  { from: 'app', to: 'db', y: 526, lines: ['7) lock · débito 100 → 75 · ledger', '+ outbox · COMMIT'] },
  { from: 'app', to: 'provider', y: 572, lines: ['8) 201 PROCESSED'], dashed: true },

  { from: 'worker', to: 'db', y: 668, lines: ['9) há pendentes vencidos?'] },
  { from: 'worker', to: 'db', y: 716, lines: ['10) lock da wallet', '+ lock da transação (SKIP LOCKED)'] },
  { from: 'worker', to: 'db', y: 764, lines: ['11) busca bet-1: existe e confere'] },
  { from: 'worker', to: 'db', y: 818, lines: ['12) PROCESSED · saldo 75 → 100', 'ledger CREDIT · outbox · COMMIT'] },
];

// ── notas ──
interface Note {
  x: number;
  y: number;
  w: number;
  h: number;
  lines: string[];
  fill?: string;
  stroke?: string;
  dashed?: boolean;
  bold?: boolean;
}
const notes: Note[] = [
  { x: 830, y: 262, w: 210, h: 78, lines: ['saldo INTOCADO', 'sem lançamento no ledger', 'evento PendingReference'] },
  { x: 830, y: 495, w: 210, h: 44, lines: ['saldo 100,00 → 75,00', '(a aposta é processada normal)'] },
  { x: 470, y: 686, w: 310, h: 52, lines: ['mesmas regras do caminho normal', '(WagerProcessor): nenhuma regra duplicada'] },
  { x: 470, y: 780, w: 310, h: 52, lines: ['o banco confere saldo == ledger no COMMIT', 'e a reversão passa a contar como feita'] },
  { x: 470, y: 846, w: 710, h: 36, lines: ['13) em outro processo: o publisher da outbox publica os eventos em wager-events.fifo (pelo menos uma vez)'] },
  {
    x: 20, y: 902, w: 1160, h: 42, dashed: true, fill: '#ffffff', stroke: '#999999',
    lines: ['E se a referência continuasse ausente? O worker reagenda com backoff 2 s · 2ⁿ (teto 5 min); após 15 tentativas (≈ 43 min) o estorno', 'vira REJECTED / REFERENCE_NOT_FOUND e sai o evento WagerTransactionRejected, para o provedor reenviar.'],
  },
];

// ── proteção: nenhum texto pode estourar o espaço ──
const est = (s: string, size: number, bold = false) => s.length * size * (bold ? 0.58 : 0.54);
function checkFit(): void {
  const errs: string[] = [];
  for (const m of msgs) {
    const span = Math.abs(X[m.to] - X[m.from]) - 24;
    for (const l of m.lines) if (est(l, 11) > span) errs.push(`msg "${l}" ~${Math.round(est(l, 11))}px > ${span}px`);
  }
  for (const n of notes) {
    for (const l of n.lines) if (est(l, 11) > n.w - 16) errs.push(`nota "${l.slice(0, 30)}…" ~${Math.round(est(l, 11))}px > ${n.w - 16}px`);
    if (n.lines.length * 14 + 10 > n.h) errs.push(`nota "${n.lines[0]}" não cabe em h=${n.h}`);
  }
  for (const p of participants) {
    for (const l of p.title) if (est(l, 11, true) > 184) errs.push(`participante "${l}" ~${Math.round(est(l, 11, true))}px > 184px`);
  }
  if (errs.length) throw new Error(`texto não cabe:\n  ${errs.join('\n  ')}`);
}

const esc = (s: string) => s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');

// ───────────── SVG ─────────────
function svgMsg(m: Msg, i: number): string {
  const x1 = X[m.from];
  const x2 = X[m.to];
  const dir = x2 > x1 ? 1 : -1;
  const sx = x1 + dir * 4;
  const ex = x2 - dir * 4;
  const mid = (x1 + x2) / 2;
  const n = m.lines.length;
  const text = m.lines
    .map((l, k) => `<text x="${mid}" y="${m.y - 7 - (n - 1 - k) * 13}" text-anchor="middle" font-size="11" fill="#1a1a1a" xml:space="preserve">${esc(l)}</text>`)
    .join('\n    ');
  return `<g id="msg-${i}">
    <line x1="${sx}" y1="${m.y}" x2="${ex}" y2="${m.y}" stroke="#333" stroke-width="1.6" ${m.dashed ? 'stroke-dasharray="6 4"' : ''} marker-end="url(#arrow)"/>
    ${text}
  </g>`;
}

function svgNote(n: Note, i: number): string {
  const blockH = n.lines.length * 14;
  const y0 = n.y + (n.h - blockH) / 2 + 11;
  const texts = n.lines
    .map((l, k) => `<text x="${n.x + n.w / 2}" y="${(y0 + k * 14).toFixed(1)}" text-anchor="middle" font-size="11" fill="#1a1a1a" xml:space="preserve">${esc(l)}</text>`)
    .join('\n    ');
  return `<g id="note-${i}">
    <rect x="${n.x}" y="${n.y}" width="${n.w}" height="${n.h}" rx="5" fill="${n.fill ?? '#fff2cc'}" stroke="${n.stroke ?? '#d6b656'}" stroke-width="1.2" ${n.dashed ? 'stroke-dasharray="6 4"' : ''}/>
    ${texts}
  </g>`;
}

function buildSvg(): string {
  const lifelines = participants
    .map((p) => `<line x1="${X[p.id]}" y1="${LIFE_TOP}" x2="${X[p.id]}" y2="${LIFE_BOTTOM}" stroke="#888" stroke-width="1.2" stroke-dasharray="5 5"/>`)
    .join('\n  ');
  const heads = participants
    .map((p) => {
      const x = X[p.id] - 100;
      const lines = p.title
        .map((l, k) => `<text x="${X[p.id]}" y="${HEADER_Y + (p.title.length === 1 ? 30 : 22 + k * 16)}" text-anchor="middle" font-size="${k === 0 ? 12 : 10}" ${k === 0 ? 'font-weight="bold"' : ''} fill="#1a1a1a">${esc(l)}</text>`)
        .join('\n    ');
      return `<g><rect x="${x}" y="${HEADER_Y}" width="200" height="${HEADER_H}" rx="7" fill="${p.fill}" stroke="${p.stroke}" stroke-width="1.5"/>
    ${lines}</g>`;
    })
    .join('\n  ');
  const bandSvg = bands
    .map((b) => `<g><rect x="10" y="${b.y}" width="${W - 20}" height="${b.h}" rx="8" fill="${b.fill}" stroke="${b.stroke}" stroke-width="1"/>
    <text x="165" y="${b.y + 18}" font-size="11" font-weight="bold" fill="#444">${esc(b.label)}</text></g>`)
    .join('\n  ');
  return `<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" width="${W}" height="${H}" viewBox="0 0 ${W} ${H}" font-family="${FONT}" role="img" aria-labelledby="title desc">
  <title id="title">Sequência: estorno que chega antes da aposta (PENDING_REFERENCE)</title>
  <desc id="desc">O REFUND chega antes da BET que referencia, é gravado como PENDING_REFERENCE sem mexer no saldo; a BET chega e é processada; o worker encontra a referência e aplica o estorno em uma transação.</desc>
  <defs>
    <marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="#333"/></marker>
  </defs>
  <rect width="${W}" height="${H}" fill="#ffffff"/>
  <text x="20" y="30" font-size="20" font-weight="bold" fill="#1a1a1a">Sequência · estorno (REFUND) que chega antes da aposta (BET)</text>
  <text x="20" y="50" font-size="12" fill="#555">cenário: wallet com 100,00 · REFUND de 25,00 referenciando bet-1 · o worker resolve quando a referência existe</text>
  ${bandSvg}
  ${lifelines}
  ${heads}
  ${notes.map(svgNote).join('\n  ')}
  ${msgs.map(svgMsg).join('\n  ')}
</svg>
`;
}

// ───────────── draw.io ─────────────
function buildDrawio(): string {
  let id = 2;
  const cells: string[] = [];
  const text = (x: number, y: number, w: number, h: number, value: string, style: string) =>
    cells.push(`<mxCell id="${id++}" value="${esc(value)}" style="text;html=1;${style}" vertex="1" parent="1"><mxGeometry x="${x}" y="${y}" width="${w}" height="${h}" as="geometry"/></mxCell>`);

  for (const b of bands) {
    cells.push(`<mxCell id="${id++}" value="" style="rounded=1;whiteSpace=wrap;html=1;fillColor=${b.fill};strokeColor=${b.stroke};arcSize=3;" vertex="1" parent="1"><mxGeometry x="10" y="${b.y}" width="${W - 20}" height="${b.h}" as="geometry"/></mxCell>`);
    text(165, b.y + 4, 600, 20, b.label, 'fontSize=11;fontStyle=1;fontColor=#444444;align=left;verticalAlign=middle;');
  }
  for (const p of participants) {
    cells.push(`<mxCell id="${id++}" value="" style="endArrow=none;dashed=1;html=1;strokeColor=#888888;strokeWidth=1.2;" edge="1" parent="1"><mxGeometry relative="1" as="geometry"><mxPoint x="${X[p.id]}" y="${LIFE_TOP}" as="sourcePoint"/><mxPoint x="${X[p.id]}" y="${LIFE_BOTTOM}" as="targetPoint"/></mxGeometry></mxCell>`);
  }
  for (const p of participants) {
    const value = p.title.map((l, k) => (k === 0 ? `<b>${esc(l)}</b>` : `<font style="font-size:10px">${esc(l)}</font>`)).join('<br>');
    cells.push(`<mxCell id="${id++}" value="${esc(value)}" style="rounded=1;whiteSpace=wrap;html=1;fillColor=${p.fill};strokeColor=${p.stroke};fontSize=12;arcSize=8;" vertex="1" parent="1"><mxGeometry x="${X[p.id] - 100}" y="${HEADER_Y}" width="200" height="${HEADER_H}" as="geometry"/></mxCell>`);
  }
  for (const n of notes) {
    cells.push(`<mxCell id="${id++}" value="${esc(n.lines.join('<br>'))}" style="rounded=1;whiteSpace=wrap;html=1;fillColor=${n.fill ?? '#fff2cc'};strokeColor=${n.stroke ?? '#d6b656'};fontSize=11;align=center;verticalAlign=middle;${n.dashed ? 'dashed=1;' : ''}arcSize=10;" vertex="1" parent="1"><mxGeometry x="${n.x}" y="${n.y}" width="${n.w}" height="${n.h}" as="geometry"/></mxCell>`);
  }
  for (const m of msgs) {
    const dir = X[m.to] > X[m.from] ? 1 : -1;
    cells.push(`<mxCell id="${id++}" value="" style="endArrow=block;endFill=1;html=1;rounded=0;strokeColor=#333333;strokeWidth=1.6;${m.dashed ? 'dashed=1;' : ''}" edge="1" parent="1"><mxGeometry relative="1" as="geometry"><mxPoint x="${X[m.from] + dir * 4}" y="${m.y}" as="sourcePoint"/><mxPoint x="${X[m.to] - dir * 4}" y="${m.y}" as="targetPoint"/></mxGeometry></mxCell>`);
    const span = Math.abs(X[m.to] - X[m.from]);
    text(Math.min(X[m.from], X[m.to]), m.y - 8 - m.lines.length * 13, span, m.lines.length * 13 + 2, m.lines.join('<br>'), 'fontSize=11;align=center;verticalAlign=bottom;whiteSpace=wrap;');
  }
  text(20, 12, 900, 30, '<b style="font-size:20px">Sequência · estorno (REFUND) que chega antes da aposta (BET)</b>', 'align=left;verticalAlign=middle;');
  text(20, 40, 900, 20, 'cenário: wallet com 100,00 · REFUND de 25,00 referenciando bet-1 · o worker resolve quando a referência existe', 'fontSize=12;fontColor=#555555;align=left;verticalAlign=middle;');

  return `<?xml version="1.0" encoding="UTF-8"?>
<mxfile host="app.diagrams.net" agent="build-sequence-diagram.ts" version="24.0.0">
  <diagram id="pending-reference-sequence" name="Sequência PENDING_REFERENCE">
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
  ['docs/sequence-pending-reference.svg', buildSvg()],
  ['docs/sequence-pending-reference.drawio', buildDrawio()],
] as const) {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, content, 'utf8');
  console.log(`escrito ${path} (${content.length} bytes)`);
}
