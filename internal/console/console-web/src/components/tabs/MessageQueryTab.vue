<template>
  <n-space vertical size="large">
    <!-- 查询表单 -->
    <n-card>
      <n-form ref="formRef" :model="formData" :rules="rules" label-placement="left" label-width="auto"
        require-mark-placement="right-hanging">
        <n-grid :cols="3" :x-gap="24">
          <n-grid-item>
            <n-form-item label="Topic" path="topic">
              <n-select v-model:value="formData.topic" :options="topicOptions" placeholder="请选择Topic"
                @update:value="handleTopicChange" />
            </n-form-item>
          </n-grid-item>
          <n-grid-item>
            <n-form-item label="分区" path="partition">
              <n-select v-model:value="formData.partition" :options="partitionOptions" placeholder="请选择分区" />
            </n-form-item>
          </n-grid-item>
          <n-grid-item>
            <n-form-item label="消息ID" path="messageId">
              <n-input v-model:value="formData.messageId" placeholder="请输入消息ID" clearable />
            </n-form-item>
          </n-grid-item>
          <n-grid-item>
            <n-form-item label="Tag" path="tag">
              <n-input v-model:value="formData.tag" placeholder="请输入Tag" clearable />
            </n-form-item>
          </n-grid-item>
        </n-grid>
      </n-form>

      <template #footer>
        <n-space justify="end">
          <n-button type="primary" @click="handleSearch">查询</n-button>
          <n-button @click="handleReset">重置</n-button>
        </n-space>
      </template>
    </n-card>

    <!-- 查询结果表格 -->
    <n-data-table :columns="columns" :data="messages" :loading="loading" :bordered="false" striped />
    <n-space justify="center" style="margin-top: 16px">
      <n-button v-if="hasMore" :loading="loading" @click="loadMore" type="primary" size="large">
        <template #icon>
          <n-icon>
            <refresh />
          </n-icon>
        </template>
        加载更多
      </n-button>
      <n-text v-else depth="3" style="font-size: 14px">
        <n-icon><information-circle /></n-icon>
        没有更多数据
      </n-text>
    </n-space>
  </n-space>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { useMessage } from 'naive-ui'
import type { DataTableColumns, FormRules, FormInst, SelectOption } from 'naive-ui'
import {
  fetchTopics,
  queryMessages,
  type Message,
  type TopicData,
} from '@/api/topicService'
import {
  NSpace,
  NCard,
  NForm,
  NFormItem,
  NInput,
  NSelect,
  NButton,
  NDataTable,
  NGrid,
  NGridItem,
} from 'naive-ui'

const message = useMessage()
const formRef = ref<FormInst | null>(null)
const loading = ref(false)
const messages = ref<Message[]>([])
const topics = ref<TopicData[]>([])
const hasMore = ref(false)
interface FormData {
  topic: string
  partition: number | null
  messageId: string
  tag: string
  pageNo: number
  pageSize: number
}

const formData = ref<FormData>({
  topic: '',
  partition: null,
  messageId: '',
  tag: '',
  pageNo: 1,
  pageSize: 10
})

const rules: FormRules = {
  topic: [
    { required: true, message: '请选择Topic' }
  ],
  partition: [
    { required: true, message: '请选择分区' }
  ]
}

const topicOptions = ref<SelectOption[]>([])
const partitionOptions = ref<SelectOption[]>([])

const columns: DataTableColumns<Message> = [
  { title: '消息ID', key: 'messageId', width: 300 },
  { title: 'Tag', key: 'tag' },
  { title: 'Key', key: 'key' },
  {
    title: '消息内容', key: 'body', ellipsis: { tooltip: true }, render(row) {
      return atob(row.body)
    }
  },
  {
    title: '时间',
    key: 'bornTime',
    width: 200,
    render(row) {
      return new Date(row.bornTime).toLocaleString()
    }
  }
]


// 加载Topic列表
const loadTopics = async () => {
  try {
    topics.value = await fetchTopics()
    topicOptions.value = topics.value.map(topic => ({
      label: topic.topic,
      value: topic.topic
    }))
  } catch (error) {
    if (error instanceof Error) {
      message.error(error.message)
    } else {
      message.error('加载Topic列表失败')
    }
  }
}

// Topic变更时加载分区列表
const handleTopicChange = async (topic: string) => {
  if (!topic) {
    partitionOptions.value = []
    formData.value.partition = null
    return
  }
  const partitionNum = topics.value.find(t => t.topic === topic)?.partitionNum
  if (partitionNum) {
    partitionOptions.value = Array.from({ length: partitionNum }, (_, i) => ({
      label: String(i),
      value: i
    }))
  }
}

// 查询消息
const handleSearch = async () => {
  formData.value.pageNo = 1
  messages.value = []
  hasMore.value = false
  await loadMessages()
}

const loadMessages = async () => {
  if (!formRef.value) return

  try {
    await formRef.value.validate()
    loading.value = true
    const list = await queryMessages({
      topic: formData.value.topic,
      partition: formData.value.partition!,
      messageId: formData.value.messageId?.trim() || undefined,
      tag: formData.value.tag?.trim() || undefined,
      pageNo: formData.value.pageNo,
      pageSize: formData.value.pageSize
    })
    if (list) {
      messages.value.push(...list)
      hasMore.value = list.length === formData.value.pageSize
    }
  } catch (error) {
    if (error instanceof Error) {
      message.error(error.message)
    } else {
      message.error('查询消息失败')
    }
  } finally {
    loading.value = false
  }
}

const loadMore = async () => {
  formData.value.pageNo++
  await loadMessages()
}

// 重置表单
const handleReset = () => {
  if (formRef.value) {
    formRef.value.restoreValidation()
  }
  formData.value = {
    topic: '',
    partition: null,
    messageId: '',
    tag: '',
    pageNo: 1,
    pageSize: 10
  }
  messages.value = []
}

onMounted(() => {
  loadTopics()
})
</script>
