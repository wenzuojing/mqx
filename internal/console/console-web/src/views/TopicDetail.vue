<template>
  <n-space vertical size="large">
    <n-page-header @back="handleBack">
      <template #title>
        Topic: {{ topic }}
      </template>
    </n-page-header>

    <n-tabs type="line" :value="activeTab" @update:value="handleTabUpdate">
      <n-tab-pane name="consumerGroups" tab="消费组">
        <consumer-groups-tab :topic="topic" />
      </n-tab-pane>
      <n-tab-pane name="partitions" tab="分区">
        <partitions-tab :topic="topic" />
      </n-tab-pane>
    </n-tabs>
  </n-space>
</template>

<script setup lang="ts">
import { ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { NSpace, NTabs, NTabPane, NPageHeader } from 'naive-ui'
import ConsumerGroupsTab from '@/components/topic-detail/ConsumerGroupsTab.vue'
import PartitionsTab from '@/components/topic-detail/PartitionsTab.vue'

const route = useRoute()
const router = useRouter()
const topic = ref(route.params.topic as string)
const activeTab = ref('consumerGroups')

const handleTabUpdate = (value: string) => {
  activeTab.value = value
}

const handleBack = () => {
  router.push('/')
}
</script>
