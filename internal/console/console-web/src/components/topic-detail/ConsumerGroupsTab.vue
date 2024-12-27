<template>
  <n-space vertical size="large">
    <n-space justify="end">
      <n-button @click="loadConsumerGroups">
        <template #icon>
          <n-icon>
            <Refresh />
          </n-icon>
        </template>
        刷新
      </n-button>
    </n-space>
    <n-data-table :columns="columns" :data="consumerGroups" :loading="loading" :pagination="pagination"
      :bordered="false" striped />

    <n-modal v-model:show="showOffsetModal" :title="selectedGroup ? `消费组 ${selectedGroup} 详情` : ''" preset="card"
      style="width: 1000px">
      <n-space justify="end" style="margin-bottom: 16px">
        <n-button @click="loadOffsets">
          <template #icon>
            <n-icon>
              <Refresh />
            </n-icon>
          </template>
          刷新
        </n-button>
      </n-space>
      <n-data-table :columns="offsetColumns" :data="offsets" :loading="offsetsLoading" :pagination="pagination"
        :bordered="false" striped />
    </n-modal>
  </n-space>
</template>

<script setup lang="ts">
import { ref, onMounted, h } from 'vue'
import { NSpace, NDataTable, NButton, NIcon, NModal, useMessage } from 'naive-ui'
import { Refresh } from '@vicons/ionicons5'
import type { DataTableColumns } from 'naive-ui'
import { getConsumerGroups, getConsumerGroupOffsets } from '@/api/topicService'
import type { ConsumerOffset } from '@/api/topicService'

const props = defineProps<{
  topic: string
}>()

const message = useMessage()
const loading = ref(false)
const consumerGroups = ref<ConsumerGroup[]>([])
const showOffsetModal = ref(false)
const selectedGroup = ref<string>('')
const offsets = ref<ConsumerOffset[]>([])
const offsetsLoading = ref(false)

interface ConsumerGroup {
  group: string
  clientCount: number
  delay: number
}

const columns: DataTableColumns<ConsumerGroup> = [
  { title: '消费组', key: 'group' },
  { title: '消费实例数', key: 'clientCount' },
  { title: '消息延迟数', key: 'delay' },
  {
    title: '操作',
    key: 'action',
    fixed: 'right',
    width: 100,
    render(row) {
      return h(NButton, { onClick: () => handleRowClick(row) }, '查看详情')
    }
  }
]

const offsetColumns: DataTableColumns<ConsumerOffset> = [
  { title: '分区', key: 'partition' },
  {
    title: '消费实例',
    key: 'instance',
    render(row) {
      return h('span', null, row.instanceId ? `${row.instanceId}@${row.hostname}(${row.active ? '活跃' : '不活跃'})` : '')
    }
  },
  { title: '最大偏移量', key: 'maxOffset' },
  { title: '最小偏移量', key: 'minOffset' },
  { title: '当前偏移量', key: 'offset' },
  {
    title: '消息延迟数',
    key: 'progress',
    render(row) {
      if (row.maxOffset == 0) {
        return h('span', null, '0')
      }
      return h('span', null, `${row.maxOffset - row.offset}`)
    }
  }
]

const pagination = {
  pageSize: 10
}

const loadConsumerGroups = async () => {
  loading.value = true
  try {
    consumerGroups.value = await getConsumerGroups(props.topic) || []
  } catch (error) {
    if (error instanceof Error) {
      message.error(error.message)
    } else {
      message.error('加载消费组信息失败')
    }
  } finally {
    loading.value = false
  }
}

const handleRowClick = async (row: ConsumerGroup) => {
  selectedGroup.value = row.group
  showOffsetModal.value = true
  await loadOffsets()

}

const loadOffsets = async () => {
  offsetsLoading.value = true
  try {
    offsets.value = await getConsumerGroupOffsets(props.topic, selectedGroup.value) || []
  } catch (error) {
    if (error instanceof Error) {
      message.error(error.message)
    } else {
      message.error('加载消费组偏移量详情失败')
    }
  } finally {
    offsetsLoading.value = false
  }
}

onMounted(() => {
  loadConsumerGroups()
})
</script>
