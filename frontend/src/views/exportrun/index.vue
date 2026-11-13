<template>
  <section class="page" data-module="exportrun">
    <header class="page-head">
      <div>
        <h2>分片输出结果</h2>
        <p class="page-desc">
          按管道规则去重、映射字段并按记录数与大小阈值封片；清单记录分片文件、记录数、字段映射和输出配置版本。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" :disabled="running" @click="runExport">
          {{ running ? '输出中…' : '执行输出' }}
        </button>
        <button class="btn" type="button" :disabled="!manifest" @click="reload">刷新清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="runExport">
      <label class="filter-item">
        <span>任务编号</span>
        <input v-model="form.task_code" placeholder="如 TASK-24" />
      </label>
      <label class="filter-item">
        <span>批次编号</span>
        <input v-model="form.batch" placeholder="如 demo" />
      </label>
      <label class="filter-item">
        <span>管道编号</span>
        <input v-model="form.pipeline_code" placeholder="如 PIPE-24" />
      </label>
      <label class="filter-item">
        <span>输出配置编号</span>
        <input v-model="form.export_code" placeholder="如 EXPORT-08" />
      </label>
      <button class="btn primary" type="submit" :disabled="running">执行输出</button>
      <button
        v-if="canResume"
        class="btn"
        type="button"
        :disabled="running"
        @click="resumeExport"
      >
        从失败分片继续
      </button>
    </form>

    <div v-if="errorMessage" class="notice error-text">{{ errorMessage }}</div>
    <div v-if="noticeMessage" class="notice">{{ noticeMessage }}</div>

    <article v-if="!manifest" class="empty-panel">
      暂无结果清单。填写任务与配置后执行输出，或选择一个已有批次查看。
    </article>

    <template v-else>
      <section class="manifest-panel">
        <header class="panel-head">
          <h3>结果清单 · {{ manifest.task_code }} / {{ manifest.batch }}</h3>
          <span class="status-tag" :class="statusClass">{{ manifest.status }}</span>
        </header>

        <p v-if="manifest.failure_reason" class="error-text">
          失败原因：{{ manifest.failure_reason }}
          <span v-if="manifest.failed_shard_index">（失败位置：分片 {{ manifest.failed_shard_index }}）</span>
        </p>

        <dl class="meta-grid">
          <div><dt>记录数</dt><dd>{{ manifest.record_count }}</dd></div>
          <div><dt>分片数</dt><dd>{{ manifest.shard_count }}</dd></div>
          <div><dt>输入条数</dt><dd>{{ manifest.input_count }}</dd></div>
          <div><dt>去重条数</dt><dd>{{ manifest.deduplicated_count }}</dd></div>
          <div><dt>无效条数</dt><dd>{{ manifest.invalid_count }}</dd></div>
          <div><dt>更新时间</dt><dd>{{ manifest.updated_at ?? '—' }}</dd></div>
        </dl>

        <div class="config-grid">
          <div>
            <h4>字段映射 · {{ manifest.pipeline?.code }} / {{ manifest.pipeline?.version }}</h4>
            <p class="muted">去重字段：{{ manifest.pipeline?.dedupe_field || '—' }}</p>
            <table class="data-table compact">
              <thead><tr><th>源字段</th><th>输出字段</th></tr></thead>
              <tbody>
                <tr v-for="(target, source) in manifest.pipeline?.field_mapping || {}" :key="source">
                  <td>{{ source }}</td>
                  <td>{{ target }}</td>
                </tr>
                <tr v-if="!mappingRows.length">
                  <td colspan="2" class="empty-state">字段映射为空</td>
                </tr>
              </tbody>
            </table>
          </div>
          <div>
            <h4>输出配置 · {{ manifest.output_config?.code }} / {{ manifest.output_config?.version }}</h4>
            <dl class="meta-list">
              <div><dt>格式</dt><dd>{{ manifest.output_config?.output_format }}</dd></div>
              <div><dt>压缩</dt><dd>{{ manifest.output_config?.compression }}</dd></div>
              <div><dt>编码</dt><dd>{{ manifest.output_config?.encoding }}</dd></div>
              <div><dt>记录数阈值</dt><dd>{{ manifest.output_config?.max_records ?? '不限' }}</dd></div>
              <div><dt>大小阈值</dt><dd>{{ formatBytes(manifest.output_config?.max_bytes) }}</dd></div>
              <div><dt>目标路径</dt><dd>{{ manifest.output_config?.target_path || '—' }}</dd></div>
            </dl>
          </div>
        </div>

        <h4>分片文件</h4>
        <table class="data-table">
          <thead>
            <tr>
              <th>序号</th>
              <th>文件名</th>
              <th>记录数</th>
              <th>大小</th>
              <th>首主键</th>
              <th>尾主键</th>
              <th>状态</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="shard in manifest.shards" :key="shard.index">
              <td>{{ shard.index }}</td>
              <td>{{ shard.file }}</td>
              <td>{{ shard.record_count }}</td>
              <td>{{ formatBytes(shard.size_bytes) }}</td>
              <td>{{ shard.first_key || '—' }}</td>
              <td>{{ shard.last_key || '—' }}</td>
              <td>{{ shard.status }}</td>
              <td>
                <a class="link" :href="shardUrl(shard.index)" target="_blank" rel="noopener">下载分片</a>
              </td>
            </tr>
            <tr v-if="!manifest.shards?.length">
              <td colspan="8" class="empty-state">空数据批次没有分片文件</td>
            </tr>
          </tbody>
        </table>

        <h4>执行记录</h4>
        <table class="data-table compact">
          <thead>
            <tr><th>次数</th><th>开始</th><th>结束</th><th>状态</th><th>恢复自</th><th>失败原因</th></tr>
          </thead>
          <tbody>
            <tr v-for="attempt in manifest.attempts" :key="attempt.attempt">
              <td>{{ attempt.attempt }}</td>
              <td>{{ attempt.started_at }}</td>
              <td>{{ attempt.finished_at || '—' }}</td>
              <td>{{ attempt.status }}</td>
              <td>{{ attempt.resumed_from_shard ?? '—' }}</td>
              <td>{{ attempt.failure_reason || '—' }}</td>
            </tr>
          </tbody>
        </table>
      </section>
    </template>

    <footer class="page-foot">
      <span>共 {{ history.length }} 个结果清单</span>
      <button class="link" type="button" @click="loadHistory">刷新批次列表</button>
    </footer>

    <table v-if="history.length" class="data-table compact">
      <thead>
        <tr>
          <th>任务</th><th>批次</th><th>状态</th><th>记录数</th><th>分片数</th>
          <th>配置版本</th><th>更新时间</th><th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="item in history" :key="`${item.task_code}/${item.batch}`">
          <td>{{ item.task_code }}</td>
          <td>{{ item.batch }}</td>
          <td>{{ item.status }}</td>
          <td>{{ item.record_count }}</td>
          <td>{{ item.shard_count }}</td>
          <td>{{ item.config_version }}</td>
          <td>{{ item.updated_at }}</td>
          <td>
            <button class="link" type="button" @click="openBatch(item.task_code, item.batch)">查看</button>
          </td>
        </tr>
      </tbody>
    </table>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Shard = {
  index: number
  file: string
  record_count: number
  size_bytes: number
  first_key?: string
  last_key?: string
  status?: string
}

type Manifest = {
  task_code: string
  batch: string
  status: string
  failure_reason?: string | null
  failed_shard_index?: number | null
  record_count: number
  shard_count: number
  input_count: number
  deduplicated_count: number
  invalid_count: number
  updated_at?: string
  pipeline?: {
    code: string
    version: string
    dedupe_field?: string
    field_mapping?: Record<string, string>
  }
  output_config?: {
    code: string
    version: string
    output_format?: string
    compression?: string
    encoding?: string
    max_records?: number | null
    max_bytes?: number | null
    target_path?: string
  }
  shards: Shard[]
  attempts: Array<{
    attempt: number
    started_at: string
    finished_at?: string | null
    status: string
    resumed_from_shard?: number | null
    failure_reason?: string | null
  }>
}

type HistoryItem = {
  task_code: string
  batch: string
  status: string
  record_count: number
  shard_count: number
  config_version: string
  updated_at: string
}

const ENDPOINT = '/api/exportrun'
const running = ref(false)
const errorMessage = ref('')
const noticeMessage = ref('')
const manifest = ref<Manifest | null>(null)
const history = ref<HistoryItem[]>([])
const form = ref({ task_code: 'TASK-24', batch: 'demo', pipeline_code: 'PIPE-24', export_code: 'EXPORT-08' })

const mappingRows = computed(() => Object.entries(manifest.value?.pipeline?.field_mapping || {}))

const stats = computed(() => [
  { label: '清单记录数', value: manifest.value?.record_count ?? 0 },
  { label: '分片数', value: manifest.value?.shard_count ?? 0 },
  { label: '本次失败条数', value: manifest.value?.invalid_count ?? 0 },
  { label: '库里批次清单', value: history.value.length },
])

const canResume = computed(() => {
  const current = manifest.value
  if (!current) return false
  // 只有写入中断（存在失败分片位置或整体失败）才需要从失败位置继续；
  // 仅有脏数据被跳过的批次没有可补写的分片。
  return current.failed_shard_index != null || current.status === '失败'
})

const statusClass = computed(() => ({
  'status-success': manifest.value?.status === '成功',
  'status-warning': manifest.value?.status === '部分成功',
  'status-error': manifest.value?.status === '失败',
}))

function formatBytes(value?: number | null) {
  if (value == null) return '不限'
  if (value < 1024) return `${value} B`
  if (value < 1024 ** 2) return `${(value / 1024).toFixed(1)} KB`
  if (value < 1024 ** 3) return `${(value / 1024 ** 2).toFixed(1)} MB`
  return `${(value / 1024 ** 3).toFixed(1)} GB`
}

function shardUrl(index: number) {
  const current = manifest.value
  if (!current) return '#'
  return `${ENDPOINT}/${current.task_code}/${current.batch}/shards/${index}`
}

async function postJson(path: string, body: Record<string, unknown>) {
  const response = await request(path, { method: 'POST', body: JSON.stringify(body) })
  const payload = await response.json().catch(() => ({ detail: '接口未返回可读结果' }))
  if (!response.ok) {
    throw new Error(String(payload.detail || `接口返回 ${response.status}`))
  }
  return payload
}

async function runExport() {
  errorMessage.value = ''
  noticeMessage.value = ''
  running.value = true
  try {
    const payload = await postJson(ENDPOINT, {
      task_code: form.value.task_code.trim(),
      pipeline_code: form.value.pipeline_code.trim(),
      export_code: form.value.export_code.trim(),
      batch: form.value.batch.trim() || 'default',
    })
    manifest.value = payload as Manifest
    noticeMessage.value = resultMessage(payload as Manifest)
    await loadHistory()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '输出执行失败'
  } finally {
    running.value = false
  }
}

async function resumeExport() {
  const current = manifest.value
  if (!current) return
  errorMessage.value = ''
  noticeMessage.value = ''
  running.value = true
  try {
    const payload = await postJson(`${ENDPOINT}/${current.task_code}/${current.batch}/resume`, {})
    manifest.value = payload as Manifest
    noticeMessage.value = resultMessage(payload as Manifest)
    await loadHistory()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '恢复执行失败'
  } finally {
    running.value = false
  }
}

function resultMessage(payload: Manifest) {
  if (payload.status === '成功') {
    return `输出完成：${payload.record_count} 条记录写入 ${payload.shard_count} 个分片`
  }
  return `输出${payload.status}：${payload.failure_reason || '部分记录未写入'}`
}

async function openBatch(taskCode: string, batch: string) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${taskCode}/${batch}`)
    if (!response.ok) throw new Error(`结果清单读取失败（${response.status}）`)
    manifest.value = (await response.json()) as Manifest
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '结果清单读取失败'
  }
}

async function loadHistory() {
  try {
    const response = await request(`${ENDPOINT}?size=50`)
    if (!response.ok) throw new Error(`批次列表读取失败（${response.status}）`)
    const payload = await response.json()
    history.value = payload.items ?? []
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '批次列表读取失败'
  }
}

async function reload() {
  const current = manifest.value
  if (!current) {
    await loadHistory()
    return
  }
  await openBatch(current.task_code, current.batch)
  await loadHistory()
}

onMounted(async () => {
  await loadHistory()
  if (history.value.length) {
    const latest = history.value[0]
    await openBatch(latest.task_code, latest.batch)
  }
})
</script>

<style scoped>
.notice { margin: 8px 0; padding: 8px 10px; background: #fff; border: 1px solid var(--border); border-radius: 6px; font-size: 13px; }
.manifest-panel { background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 12px 14px; margin: 8px 0 14px; }
.panel-head { display: flex; align-items: center; justify-content: space-between; }
.status-tag { font-size: 12px; padding: 2px 8px; border-radius: 999px; background: #eef2ff; color: #3949ab; }
.status-success { background: #ecfdf3; color: #027a48; }
.status-warning { background: #fffaeb; color: #b54708; }
.status-error { background: #fef3f2; color: #b42318; }
.meta-grid { display: grid; grid-template-columns: repeat(6, minmax(0, 1fr)); gap: 8px; margin: 10px 0; }
.meta-grid div dt, .meta-list dt { color: var(--muted); font-size: 12px; }
.meta-grid div dd { margin: 2px 0 0; font-size: 16px; font-weight: 600; }
.config-grid { display: grid; grid-template-columns: 1.2fr 1fr; gap: 16px; margin: 10px 0; }
.meta-list div { display: flex; justify-content: space-between; font-size: 13px; padding: 3px 0; }
.meta-list div dd { margin: 0; }
.empty-panel { background: #fff; border: 1px dashed var(--border); border-radius: 8px; padding: 24px; text-align: center; color: var(--muted); }
.muted { color: var(--muted); font-size: 12px; }
.compact th, .compact td { padding: 6px 8px; }
h4 { margin: 14px 0 6px; font-size: 14px; }
</style>
