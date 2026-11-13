<template>
  <div class="app-shell">
    <aside class="app-side">
      <h1 class="app-title">网络数据采集管理平台</h1>
      <nav class="nav-list">
        <RouterLink v-for="item in navItems" :key="item.path" :to="item.path" class="nav-item">
          {{ item.label }}
        </RouterLink>
      </nav>
    </aside>
    <main class="app-main">
      <header class="app-head">
        <span class="head-desc">面向网页数据接入、定时增量采集、抽取规则管理、代理调度、限速策略、失败重试与采集结果验证的网络数据采集工程管理平台。</span>
        <span class="head-user">当前值班：{{ store.operator }} · {{ store.shiftLabel }}</span>
      </header>
      <RouterView />
    </main>
  </div>
</template>

<script setup lang="ts">
import { useSessionStore } from '@/stores/session'

const store = useSessionStore()

const navItems = [{ label: "运营概览", path: "/" }, { label: "采集源登记", path: "/datasource" }, { label: "解析规则", path: "/parserule" }, { label: "代理调度", path: "/proxyconfig" }, { label: "限速策略", path: "/ratelimiter" }, { label: "采集任务", path: "/taskcrawl" }, { label: "预处理管道", path: "/pipeline" }, { label: "RPA流程编排", path: "/robotflow" }, { label: "失败重试", path: "/retrypolicy" }, { label: "结果校验", path: "/resultcheck" }, { label: "采集告警", path: "/alertrule" }, { label: "诊断记录", path: "/diagnose" }, { label: "调度计划", path: "/scheduler" }, { label: "输出配置", path: "/exportconf" }, { label: "登录态管理", path: "/lmcfg" }, { label: "反爬对抗", path: "/blacklist" }, { label: "采集日志", path: "/storagelog" }, { label: "模板库", path: "/templatelib" }, { label: "数据标注", path: "/datalabel" }, { label: "分片输出结果", path: "/exportrun" }]
</script>
