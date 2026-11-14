#!/usr/bin/env node
// Copyright (c) Meta Platforms, Inc. and affiliates.
'use strict';

const assert = require('node:assert/strict');
const { createHash } = require('node:crypto');
const fs = require('node:fs');
const { createRequire } = require('node:module');
const path = require('node:path');

const args = process.argv.slice(2);
function argument(name) {
  const index = args.indexOf(name);
  assert(index >= 0 && args[index + 1], `Missing ${name}`);
  return path.resolve(args[index + 1]);
}
const upstream = argument('--upstream');
const output = argument('--output');
const upstreamRequire = createRequire(path.join(upstream, 'package.json'));
upstreamRequire('tsx/cjs');
const { LLParse } = upstreamRequire('llparse');
const frontend = upstreamRequire('llparse-frontend');
const { HTTP } = upstreamRequire('./src/llhttp/http.ts');
const constants = upstreamRequire('./src/llhttp/constants.ts');
const revision = '0ae8c68c3c5a8fce13dbff87190e1ba40d8f9666';
assert.equal(upstreamRequire('./package.json').version, '9.4.4');
for (const [name, version] of Object.entries({llparse:'7.3.1', 'llparse-frontend':'3.0.0', 'llparse-builder':'1.5.2'})) {
  assert.equal(upstreamRequire(`${name}/package.json`).version, version, `Unexpected ${name} version`);
}
const helperHashes = {
  'src/native/http.c': 'a1f2b23168f8e9b5bfa464c89027d3ca259fc649ae3d7e72bfff997d1aeb5d72',
  'src/native/api.c': '0f8590206fe2f264db2825401b5fb856b979ee1363dc88b06f36dc9577afb941',
};
for (const [file, expected] of Object.entries(helperHashes)) {
  const actual = createHash('sha256').update(fs.readFileSync(path.join(upstream,file))).digest('hex');
  assert.equal(actual, expected, `Review and update the Rust helper port before regenerating: ${file}`);
}
for (const [file, expected] of Object.entries({
  'src/llhttp/http.ts': '7fef7e6e7a18e3caaa54e2815836803ad9ac63ee8f049493e80c60050abcf3d1',
  'src/llhttp/constants.ts': '5e53f9a8d307961d05033873548d46567eb2c145f00e8d295f08e90d2606011c',
  'src/llhttp/url.ts': 'b43ef8726267f4fa8b06b6e7400225a5a8277bf90348192b3c72658b21d32fa6',
})) {
  const actual = createHash('sha256').update(fs.readFileSync(path.join(upstream,file))).digest('hex');
  assert.equal(actual, expected, `Unexpected grammar source for pinned revision: ${file}`);
}
const parser = new LLParse('llhttp');
const entry = new HTTP(parser).build().entry;
const container = new frontend.Container();
const empty = container.build();
const implementation = {};
for (const family of ['node','code','transform']) {
  implementation[family] = Object.fromEntries(Object.keys(empty[family]).map(kind => [kind, class {
    constructor(ref) { this.ref = ref; this.kind = kind; }
  }]));
}
container.add('rust', implementation);
const info = new frontend.Frontend('llhttp', container.build()).compile(entry, parser.properties);
assert.equal(info.spans.length, 1, 'The runtime must be extended for concurrent spans');
const expectedProperties = {
  content_length:'i64', type:'i8', method:'i8', http_major:'i8', http_minor:'i8',
  header_state:'i8', lenient_flags:'i16', upgrade:'i8', finish:'i8', flags:'i16',
  status_code:'i16', initial_message_completed:'i8', settings:'ptr',
};
assert.deepEqual(Object.fromEntries(parser.properties.map(p => [p.name,p.ty])),expectedProperties);
const nodes = new Set([info.root]);
for (const node of nodes) for (const slot of node.ref.getSlots()) nodes.add(slot.node);
const nodeList = [...nodes];
// Every fused node has one structural incoming edge and cannot be resumed.
// Its body is emitted at that edge, so code is moved rather than duplicated.
const incoming = new Map(nodeList.map(node => [node, 0]));
for (const node of nodeList) {
  for (const slot of node.ref.getSlots()) incoming.set(slot.node, incoming.get(slot.node) + 1);
}
const fused = new Set(nodeList.filter(node => incoming.get(node) === 1 && !info.resumptionTargets.has(node)));
const dispatchNodes = nodeList.filter(node => !fused.has(node));
for (const node of fused) {
  assert(['Invoke', 'SpanEnd', 'Pause', 'Error'].includes(node.get('rust').kind),
    `Unexpected resumable node selected for fusion: ${node.ref.id.name}`);
}
const bodyEmissions = new Map(nodeList.map(node => [node, 0]));
let maxFusionDepth = 0;
function checkFusion(node, active = new Set()) {
  assert(!active.has(node), 'Non-resumable fusion cycle');
  const next = new Set(active).add(node);
  maxFusionDepth = Math.max(maxFusionDepth, active.size);
  for (const slot of node.ref.getSlots()) if (fused.has(slot.node)) checkFusion(slot.node, next);
}
for (const node of dispatchNodes) checkFusion(node);
assert(maxFusionDepth <= 16, `Unexpected fusion depth: ${maxFusionDepth}`);
// Collapse fused paths before ordering retained states.
const successors = new Map(dispatchNodes.map(node => [node, new Set()]));
function collectTargets(node, targets) {
  for (const slot of node.ref.getSlots()) {
    if (fused.has(slot.node)) collectTargets(slot.node, targets);
    else targets.add(slot.node);
  }
}
for (const node of dispatchNodes) collectTargets(node, successors.get(node));
const visited = new Set();
const postorder = [];
function visitOrder(node) {
  if (visited.has(node)) return;
  visited.add(node);
  for (const child of successors.get(node)) visitOrder(child);
  postorder.push(node);
}
assert(!fused.has(info.root));
visitOrder(info.root);
assert.equal(postorder.length, dispatchNodes.length, 'Unreachable retained state');
const emissionOrder = postorder.reverse();
const bodyOrdinals = new Map(emissionOrder.map((node, i) => [node, i]));
const blockLabel = node => {
  assert(bodyOrdinals.has(node));
  return `bb_${bodyOrdinals.get(node)}`;
};
let currentOrdinal = -1;
const openLabels = new Set();
const emissionStack = [];
let nextFrameLabel = 0;
let labeledLoopBodies = 0;
let directBackedgeSites = 0;
let forwardEdgeSites = 0;
let backwardDispatchSites = 0;
let blockScanCopies = 0;
const stateIds = new Map(nodeList.map((n,i) => [n,`State::S${i}`]));
const state = n => { assert(stateIds.has(n)); assert(!fused.has(n), 'Fused node cannot be a continuation'); return stateIds.get(n); };
const field = n => `self.${({type:'parser_type',finish:'finish_state'})[n] || n}`;
const type = n => { assert(expectedProperties[n] !== 'ptr'); return expectedProperties[n].replace('i','u'); };
const quoted = s => JSON.stringify(s).replace(/\\u([0-9a-fA-F]{4})/g, '\\u{$1}');
const eventName = name => {
  assert(name.startsWith('llhttp__on_'), `Unknown callback: ${name}`);
  return name.slice('llhttp__on_'.length).split('_').map(s=>s[0].toUpperCase()+s.slice(1)).join('');
};
const events = new Map();
for (const node of nodeList) {
  const {kind,ref} = node.get('rust');
  const callbacks = kind === 'Invoke' ? [ref.code] : ['SpanStart','SpanEnd'].includes(kind) ? [ref.callback] : [];
  for (const wrapped of callbacks) {
    const code = wrapped.get('rust');
    if (code.ref.name.startsWith('llhttp__on_')) events.set(eventName(code.ref.name),code.ref.name.slice('llhttp__'.length));
  }
}
const out=[];
const emit = s => out.push(s);
emit('// Copyright (c) Meta Platforms, Inc. and affiliates.');
emit('// Derived from llhttp, Copyright (c) 2018 Fedor Indutny.');
emit('// SPDX-License-Identifier: MIT');
emit('// @generated by fbcode/roxygen/codec/http1/generator/generate.cjs. Do not edit.');
emit(`// llhttp 9.4.4 (${revision}); llparse 7.3.1, frontend 3.0.0, builder 1.5.2.`);
emit('// Regenerate: node fbcode/roxygen/codec/http1/generator/generate.cjs --upstream CHECKOUT --output MODULE_DIRECTORY');
emit('');
emit('#![allow(dead_code)]');
emit('use std::ops::Range;');
emit('use super::Parser;');
emit('');
emit('#[derive(Clone, Copy, Debug, Eq, PartialEq)]');
emit('pub(crate) struct Error(pub(crate) i32);');
emit('impl Error {');
for (const [name,value] of Object.entries(constants.ERROR)) emit(`pub(crate) const ${name}: Self = Self(${value});`);
emit("pub(crate) fn name(self) -> &'static str { match self.0 {");
for (const [name,value] of Object.entries(constants.ERROR)) emit(`${value} => "HPE_${name}",`);
emit('_ => "HPE_UNKNOWN", } }');
emit('}');
for (const [module,items,ty] of [['flags',constants.FLAGS,'u16'],['lenient_flags',constants.LENIENT_FLAGS,'u16'],['methods',constants.METHODS,'u8'],['finish',constants.FINISH,'u8']]) {
  emit(`pub(crate) mod ${module} {`);
  for (const [name,value] of Object.entries(items)) emit(`pub(crate) const ${name.replaceAll('-','_')}: ${ty} = ${value};`);
  emit('}');
}
emit('#[derive(Clone, Copy, Debug, Eq, PartialEq)]');
emit('#[repr(u8)]');
emit(`pub(crate) enum ParserType { Both = ${constants.TYPE.BOTH}, Request = ${constants.TYPE.REQUEST}, Response = ${constants.TYPE.RESPONSE} }`);
emit('#[derive(Clone, Copy, Debug, Eq, PartialEq)]');
emit('pub(crate) enum Event {');
for (const name of events.keys()) emit(`${name},`);
emit('}');
emit('impl Event {');
emit("pub(crate) fn name(self) -> &'static str { match self {");
for (const [name,original] of events) emit(`Self::${name} => "${original}",`);
emit('} }');
emit("pub(super) fn span_error_reason(self) -> &'static str { match self {");
for (const [name,original] of events) emit(`Self::${name} => \"Span callback error in ${original}\",`);
emit('} }');
emit('}');
emit('#[derive(Clone, Copy, Debug, Eq, PartialEq)]');
emit('pub(super) enum State {');
for (const [i,node] of nodeList.entries()) if (!fused.has(node)) emit(`S${i}, // ${node.ref.id.name}`);
emit('Failed,');
emit('}');
emit(`pub(super) const ROOT_STATE: State = ${state(info.root)};`);
emit('impl Parser {');
emit("pub(crate) fn method_name(&self) -> &'static str { match self.method {");
for (const [name,value] of Object.entries(constants.METHODS)) emit(`${value} => ${quoted(name)},`);
emit('_ => "", } }');
function transform(ref,value) {
  const {kind}=ref.get('rust');
  if (kind==='ID') return value;
  if (kind==='ToLower') return `(${value}).to_ascii_lowercase()`;
  if (kind==='ToLowerUnsafe') return `((${value}) | 0x20)`;
  throw new Error(`Unknown transform ${kind}`);
}
function tail(edge) {
  assert(edge);
  if (!edge.noAdvance) emit('pos += 1;');
  if (edge.value !== undefined) emit(`match_value = ${edge.value};`);
  assert(emissionStack.length > 0 && currentOrdinal >= 0);
  const ancestor = emissionStack.findLast(item => item.node === edge.node);
  if (ancestor) {
    assert(!fused.has(edge.node), 'Mandatory fusion cycle');
    ancestor.loopTarget = true;
    directBackedgeSites += 1;
    emit(`continue '${ancestor.label};`);
    return;
  }
  if (fused.has(edge.node)) {
    emit(`{ // fused ${edge.node.ref.id.name}`);
    emitNodeBody(edge.node);
    emit('}');
    return;
  }
  const current = emissionOrder[currentOrdinal];
  assert(successors.get(current).has(edge.node), 'Edge absent from collapsed graph');
  const targetOrdinal = bodyOrdinals.get(edge.node);
  assert(targetOrdinal !== undefined && targetOrdinal !== currentOrdinal);
  if (targetOrdinal > currentOrdinal) {
    assert(openLabels.has(targetOrdinal), 'Forward label must enclose current body');
    forwardEdgeSites += 1;
    emit(`break '${blockLabel(edge.node)};`);
  } else {
    assert(!openLabels.has(targetOrdinal), 'Backward label is already closed');
    backwardDispatchSites += 1;
    emit(`state = ${state(edge.node)};`);
    emit("continue 'run;");
  }
}
function suspend() {
  const node = emissionStack[emissionStack.length - 1].node;
  assert(info.resumptionTargets.has(node), 'Input exhaustion needs a resumable state');
  emit(`state = ${state(node)};`);
  emit("break 'run;");
}
function prologue() {
  // This also establishes the strict bound for the following input[pos] load.
  emit('if pos >= input.len() {');
  suspend();
  emit('}');
}
function action(wrapped) {
  const {kind,ref}=wrapped.get('rust');
  const f=ref.field ? field(ref.field) : undefined;
  switch(kind) {
    case 'Load': emit(`${f} as i32`); break;
    case 'Store': emit(`${f} = match_value as ${type(ref.field)}; 0`); break;
    case 'Update': emit(`${f} = (${ref.value}i64) as ${type(ref.field)}; 0`); break;
    case 'And': emit(`${f} &= (${ref.value}i64) as ${type(ref.field)}; 0`); break;
    case 'Or': emit(`${f} |= (${ref.value}i64) as ${type(ref.field)}; 0`); break;
    case 'IsEqual': emit(`i32::from(${f} == (${ref.value}i64) as ${type(ref.field)})`); break;
    case 'Test': emit(`i32::from((${f} & (${ref.value}i64) as ${type(ref.field)}) == (${ref.value}i64) as ${type(ref.field)})`); break;
    case 'MulAdd': {
      const o=ref.options;
      assert(Number.isSafeInteger(o.base) && o.base>0);
      const ty=o.signed ? expectedProperties[ref.field] : type(ref.field);
      const cast=o.signed ? `(${f} as ${ty})` : f;
      const max=`${ty}::MAX as i128`;
      const min=`${ty}::MIN as i128`;
      emit(`let value = ${cast} as i128;`);
      emit(`let min = ${min}; let max = ${max};`);
      emit(`if value > max / ${o.base} || value < min / ${o.base} { 1 } else {`);
      emit(`let product = value * ${o.base};`);
      emit(`${f} = product as ${type(ref.field)};`);
      emit('let sum = product + i128::from(match_value);');
      emit('if sum > max || sum < min { 1 } else {');
      emit(`${f} = sum as ${type(ref.field)};`);
      if(o.max!==undefined) { assert(Number.isSafeInteger(o.max)); emit(`i32::from(sum > ${o.max})`); } else emit('0');
      emit('} }');
      break;
    }
    case 'Match':
      if (ref.name==='llhttp__before_headers_complete') emit('self.before_headers_complete()');
      else if(ref.name==='llhttp__after_headers_complete') emit('self.after_headers_complete()');
      else if(ref.name==='llhttp__after_message_complete') emit('self.after_message_complete()');
      else emit(`callback(self, Event::${eventName(ref.name)}, pos..pos)`);
      break;
    default: throw new Error(`Unsupported action ${kind} (${ref.name})`);
  }
}
emit('#[rustfmt::skip]');
emit('pub(super) fn run<F>(&mut self, input: &[u8], callback: &mut F) -> Result<(), usize>');
emit('where F: FnMut(&mut Parser, Event, Range<usize>) -> i32 {');
emit('let mut pos = 0;');
emit('let mut match_value: i32 = 0;');
emit('let mut state = self.state;');
emit("'run: loop {");
// Block j closes just before body j, so bodies i < j can break to its label.
for (let ordinal = emissionOrder.length - 1; ordinal >= 0; ordinal--) {
  emit(`'${blockLabel(emissionOrder[ordinal])}: {`);
  openLabels.add(ordinal);
}
emit('match state {');
for (const node of dispatchNodes) emit(`${state(node)} => { break '${blockLabel(node)}; },`);
emit('State::Failed => { self.error = Error::INTERNAL; self.reason = "Invalid parser continuation"; return Err(pos); }');
emit('}');
for (const [ordinal, node] of emissionOrder.entries()) {
  assert(openLabels.has(ordinal));
  emit(`} // ${blockLabel(node)}`);
  openLabels.delete(ordinal);
  currentOrdinal = ordinal;
  emit(`// ${state(node)}: ${node.ref.id.name}`);
  emitNodeBody(node);
  assert.equal(emissionStack.length, 0);
}
assert.equal(openLabels.size, 0);
function emitNodeBody(node) {
  const frame = {
    node, label: `node_${nextFrameLabel++}`, loopTarget: false,
    openingLine: out.length,
  };
  emissionStack.push(frame);
  // Patch this into a labeled loop only if an emitted graph edge targets it.
  emit('{');
  bodyEmissions.set(node, bodyEmissions.get(node) + 1);
  const {kind,ref}=node.get('rust');
  switch(kind) {
    case 'Empty': if(!ref.otherwise.noAdvance) prologue(); tail(ref.otherwise); break;
    case 'Single':
      prologue(); emit(`match ${transform(ref.transform,'input[pos]')} {`);
      for(const edge of ref.edges) { emit(`${edge.key} => {`); tail(edge); emit('}'); }
      emit('_ => {'); tail(ref.otherwise); emit('} }'); break;
    case 'TableLookup': {
      const table=new Array(256).fill(0);
      ref.edges.forEach((edge,i)=>edge.keys.forEach(key=>{assert.equal(table[key],0);table[key]=i+1;}));
      emit(`const TABLE: [u8; 256] = [${table.join(',')}];`);
      const selfLoop=ref.edges.findIndex(e=>e.node===node && !e.noAdvance);
      if (selfLoop >= 0 && ref.edges.length === 1 && ref.transform.get('rust').kind === 'ID') {
        const ranges = [];
        for (const key of ref.edges[selfLoop].keys) {
          const last = ranges[ranges.length - 1];
          if (last && last[1] + 1 === key) last[1] = key;
          else ranges.push([key, key]);
        }
        if (ranges.length <= 3) {
          for (let byte = 0; byte < 256; byte++) {
            assert.equal(ranges.some(([low, high]) => low <= byte && byte <= high),
              table[byte] === selfLoop + 1, 'Block predicate differs from the scalar table');
          }
          blockScanCopies += 1;
          const valid = byte => ranges.map(([low, high]) => {
            if (low === high) return `(${byte} == ${low})`;
            if (low === 0) return `(${byte} <= ${high})`;
            if (high === 255) return `(${byte} >= ${low})`;
            return `(${byte}.wrapping_sub(${low}) <= ${high-low})`;
          }).join(' | ');
          emit('while input.len() - pos >= 16 {');
          emit('let block = &input[pos..pos + 16];');
          emit(`let valid = ${Array.from({length:16}, (_,i)=>`(${valid(`block[${i}]`)})`).join(' & ')};`);
          emit('if !valid { break; }');
          emit('pos += 16;');
          emit('}');
        }
      }
      if(selfLoop>=0) emit(`while pos < input.len() && TABLE[${transform(ref.transform,'input[pos]')} as usize] == ${selfLoop+1} { pos += 1; }`);
      prologue(); emit(`match TABLE[${transform(ref.transform,'input[pos]')} as usize] {`);
      ref.edges.forEach((edge,i)=>{emit(`${i+1} => {`);tail(edge);emit('}');});
      emit('_ => {');tail(ref.otherwise);emit('} }');break;
    }
    case 'Sequence':
      prologue(); emit(`const SEQUENCE: &[u8] = &[${[...ref.select].join(',')}];`);
      emit('let mut index = self.sequence_index;');
      emit(`while pos < input.len() && ${transform(ref.transform,'input[pos]')} == SEQUENCE[index] {`);
      emit('index += 1;');
      emit('if index == SEQUENCE.len() {');
      emit('self.sequence_index = 0;');tail({node:ref.edge.node,value:ref.edge.value,noAdvance:false});emit('}');
      emit('pos += 1;');emit('}');
      emit('if pos >= input.len() {');emit('self.sequence_index = index;');suspend();emit('}');
      emit('self.sequence_index = 0;');tail(ref.otherwise);break;
    case 'Consume':
      emit('let available = (input.len() - pos) as u64;');
      emit(`if available >= ${field(ref.field)}${type(ref.field) === 'u64' ? '' : ' as u64'} {`);
      emit(`pos += ${field(ref.field)} as usize; ${field(ref.field)} = 0;`);
      tail(ref.otherwise);emit('}');
      emit(`${field(ref.field)} -= available${type(ref.field) === 'u64' ? '' : ' as ' + type(ref.field)};`);suspend();break;
    case 'SpanStart':
      assert.equal(ref.field.index,0);prologue();emit('self.span_start = pos;');
      emit(`self.span_callback = Some(Event::${eventName(ref.callback.ref.name)});`);tail(ref.otherwise);break;
    case 'SpanEnd':
      assert.equal(ref.field.index,0);emit('let start = self.span_start; self.span_callback = None;');
      emit(`let error = self.span(callback, Event::${eventName(ref.callback.ref.name)}, start..pos);`);
      emit('if error != 0 {');emit('self.error = Error(error);');emit(`self.state = ${state(ref.otherwise.node)};`);
      emit(`return Err(pos${ref.otherwise.noAdvance?'':' + 1'});`);emit('}');tail(ref.otherwise);break;
    case 'Invoke':
      if (ref.edges.length === 0) {
        emit('let _ = {');action(ref.code);emit('};');tail(ref.otherwise);
      } else {
        emit('let result = {');action(ref.code);emit('};');emit('match result {');
        for(const edge of ref.edges) {emit(`${edge.code} => {`);tail({node:edge.node,noAdvance:true});emit('}');}
        emit('_ => {');tail(ref.otherwise);emit('} }');
      }
      break;
    case 'Pause': case 'Error':
      emit(`self.error = Error(${ref.code}); self.reason = ${quoted(ref.reason)};`);
      emit(`self.state = ${kind==='Pause'?state(ref.otherwise.node):'State::Failed'};`);emit('return Err(pos);');break;
    default: throw new Error(`Unsupported node ${kind}`);
  }
  emit('}');
  if (frame.loopTarget) {
    out[frame.openingLine] = `'${frame.label}: loop { // ${node.ref.id.name}`;
    labeledLoopBodies += 1;
  }
  assert.equal(emissionStack.pop(), frame);
}
for (const [node, count] of bodyEmissions) assert.equal(count, 1, `Node must be emitted once: ${node.ref.id.name}`);
const emittedBodies = [...bodyEmissions.values()].reduce((a,b) => a+b,0);
assert.equal(emittedBodies, nodeList.length);
emit('}');
emit('self.state = state;');
emit('Ok(())');emit('}');emit('}');
fs.mkdirSync(output,{recursive:true});
fs.writeFileSync(path.join(output,'generated.rs'),out.join('\n')+'\n');
const statistics = {
  nodes: nodeList.length, dispatchArms: dispatchNodes.length, fused: fused.size,
  emittedBodies, extraBodies: 0, blockLabels: emissionOrder.length, maxFusionDepth,
  forwardEdgeSites, backwardDispatchSites, labeledLoopBodies, directBackedgeSites,
  blockScanCopies,
};
console.log(JSON.stringify(statistics));
