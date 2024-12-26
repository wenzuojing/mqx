<template>
  <n-tabs type="bar" :value="activeTab" @update:value="handleTabUpdate" class="nav-tabs">
    <n-tab-pane name="topic" tab="Topic管理">
      <topic-tab />
    </n-tab-pane>

    <n-tab-pane name="message" tab="消息查询">
      <message-query-tab />
    </n-tab-pane>

  </n-tabs>
</template>

<script setup lang="ts">
import { ref } from 'vue'
import { NTabs, NTabPane } from 'naive-ui'
import TopicTab from '@/components/tabs/TopicTab.vue'
import MessageQueryTab from '@/components/tabs/MessageQueryTab.vue'

// 标签页状态管理
const activeTab = ref('topic')
const handleTabUpdate = (value: string) => {
  activeTab.value = value
}
</script>

<style scoped>
.nav-tabs {
  margin-bottom: 20px;
}
</style>
