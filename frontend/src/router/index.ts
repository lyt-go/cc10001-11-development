import { createRouter, createWebHistory } from 'vue-router'

import Overview from '@/views/Overview.vue'
const Datasource = () => import('@/views/datasource/index.vue')
const Parserule = () => import('@/views/parserule/index.vue')
const Proxyconfig = () => import('@/views/proxyconfig/index.vue')
const Ratelimiter = () => import('@/views/ratelimiter/index.vue')
const Taskcrawl = () => import('@/views/taskcrawl/index.vue')
const Pipeline = () => import('@/views/pipeline/index.vue')
const Robotflow = () => import('@/views/robotflow/index.vue')
const Retrypolicy = () => import('@/views/retrypolicy/index.vue')
const Resultcheck = () => import('@/views/resultcheck/index.vue')
const Alertrule = () => import('@/views/alertrule/index.vue')
const Diagnose = () => import('@/views/diagnose/index.vue')
const Scheduler = () => import('@/views/scheduler/index.vue')
const Exportconf = () => import('@/views/exportconf/index.vue')
const Lmcfg = () => import('@/views/lmcfg/index.vue')
const Blacklist = () => import('@/views/blacklist/index.vue')
const Storagelog = () => import('@/views/storagelog/index.vue')
const Templatelib = () => import('@/views/templatelib/index.vue')
const Datalabel = () => import('@/views/datalabel/index.vue')
const Exportrun = () => import('@/views/exportrun/index.vue')

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    { path: '/datasource', name: 'datasource', component: Datasource },
    { path: '/parserule', name: 'parserule', component: Parserule },
    { path: '/proxyconfig', name: 'proxyconfig', component: Proxyconfig },
    { path: '/ratelimiter', name: 'ratelimiter', component: Ratelimiter },
    { path: '/taskcrawl', name: 'taskcrawl', component: Taskcrawl },
    { path: '/pipeline', name: 'pipeline', component: Pipeline },
    { path: '/robotflow', name: 'robotflow', component: Robotflow },
    { path: '/retrypolicy', name: 'retrypolicy', component: Retrypolicy },
    { path: '/resultcheck', name: 'resultcheck', component: Resultcheck },
    { path: '/alertrule', name: 'alertrule', component: Alertrule },
    { path: '/diagnose', name: 'diagnose', component: Diagnose },
    { path: '/scheduler', name: 'scheduler', component: Scheduler },
    { path: '/exportconf', name: 'exportconf', component: Exportconf },
    { path: '/lmcfg', name: 'lmcfg', component: Lmcfg },
    { path: '/blacklist', name: 'blacklist', component: Blacklist },
    { path: '/storagelog', name: 'storagelog', component: Storagelog },
    { path: '/templatelib', name: 'templatelib', component: Templatelib },
    { path: '/datalabel', name: 'datalabel', component: Datalabel },
    { path: '/exportrun', name: 'exportrun', component: Exportrun },
  ],
})

export default router
