<template>
  <n-space vertical size="large">
    <n-space justify="end">
      <n-button @click="loadPartitions">
        <template #icon>
          <n-icon>
            <Refresh />
          </n-icon>
        </template>
        刷新
      </n-button>
    </n-space>
    <n-data-table :columns="columns" :data="partitions" :loading="loading" :pagination="pagination" :bordered="false"
      striped />
  </n-space>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { NSpace, NDataTable, NButton, NIcon, useMessage } from 'naive-ui'
import { Refresh } from '@vicons/ionicons5'
import type { DataTableColumns } from 'naive-ui'
import { getPartitions } from '@/api/topicService'

const props = defineProps<{
  topic: string
}>()

const message = useMessage()
const loading = ref(false)
const partitions = ref<Partition[]>([])

interface Partition {
  partition: number
  stat: {
    maxOffset: number
    minOffset: number
    total: number
  }
}

const columns: DataTableColumns<Partition> = [
  { title: '分区', key: 'partition' },
  { title: '最大偏移量', key: 'stat.maxOffset' },
  { title: '最小偏移量', key: 'stat.minOffset' },
  { title: '总消息量', key: 'stat.total' }
]

const pagination = {
  pageSize: 10
}

const loadPartitions = async () => {
  loading.value = true
  try {
    partitions.value = await getPartitions(props.topic)
  } catch (error) {
    if (error instanceof Error) {
      message.error(error.message)
    } else {
      message.error('加载分区信息失败')
    }
  } finally {
    loading.value = false
  }
}

onMounted(() => {
  loadPartitions()
})
</script>
