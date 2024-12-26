<template>
  <n-space vertical size="large">
    <!-- 顶部操作栏 -->
    <n-space align="center" justify="space-between">
      <n-space>
        <n-button type="primary" @click="handleCreateTopic">
          创建Topic
        </n-button>
      </n-space>
      <n-space align="center">
        <n-input v-model:value="searchValue" placeholder="搜索Topic名称" style="width: 200px">
          <template #suffix>
            <n-icon>
              <Search />
            </n-icon>
          </template>
        </n-input>
        <n-button-group>
          <n-button @click="loadTopics">
            <n-icon>
              <Refresh />
            </n-icon>
          </n-button>
        </n-button-group>
      </n-space>
    </n-space>

    <!-- Topic列表表格 -->
    <n-data-table :columns="columns" :data="filteredData" :pagination="pagination" :bordered="false" striped
      :loading="loading" />

    <!-- 创建Topic对话框 -->
    <n-modal v-model:show="showCreateDialog" preset="card" title="创建Topic" style="width: 600px">
      <n-form ref="formRef" :model="formData" :rules="rules" label-placement="left" label-width="auto"
        require-mark-placement="right-hanging" size="medium">
        <n-form-item label="Topic名称" path="topic">
          <n-input v-model:value="formData.topic" placeholder="请输入Topic名称" />
        </n-form-item>
        <n-form-item label="分区数" path="partitionNum">
          <n-input-number v-model:value="formData.partitionNum" placeholder="请输入分区数" />
        </n-form-item>
        <n-form-item label="消息保留时长(天)" path="retentionDays">
          <n-input-number v-model:value="formData.retentionDays" placeholder="请输入消息保留时长" />
        </n-form-item>
      </n-form>
      <template #footer>
        <n-space justify="end">
          <n-button @click="handleCancelCreate">取消</n-button>
          <n-button type="primary" @click="handleConfirmCreate">确认</n-button>
        </n-space>
      </template>
    </n-modal>

    <!-- 修改Topic配置对话框 -->
    <n-modal v-model:show="showEditDialog" preset="card" title="修改Topic配置" style="width: 600px">
      <n-form ref="formRef" :model="formData" :rules="rules" label-placement="left" label-width="auto"
        require-mark-placement="right-hanging" size="medium">
        <n-form-item label="Topic名称">
          <n-input v-model:value="formData.topic" disabled />
        </n-form-item>
        <n-form-item label="分区数" path="partitionNum">
          <n-input-number v-model:value="formData.partitionNum" placeholder="请输入分区数" />
        </n-form-item>
        <n-form-item label="消息保留时长(天)" path="retentionDays">
          <n-input-number v-model:value="formData.retentionDays" placeholder="请输入消息保留时长" />
        </n-form-item>
      </n-form>
      <template #footer>
        <n-space justify="end">
          <n-button @click="handleCancelEdit">取消</n-button>
          <n-button type="primary" @click="handleConfirmEdit">确认</n-button>
        </n-space>
      </template>
    </n-modal>

    <!-- 发送消息对话框 -->
    <n-modal v-model:show="showSendDialog" preset="card" title="发送消息" style="width: 600px">
      <n-form ref="sendFormRef" :model="sendFormData" :rules="sendRules" label-placement="left" label-width="auto"
        require-mark-placement="right-hanging" size="medium">
        <n-form-item label="Tag" path="tag">
          <n-input v-model:value="sendFormData.tag" placeholder="请输入Tag" />
        </n-form-item>
        <n-form-item label="Key" path="key">
          <n-input v-model:value="sendFormData.key" placeholder="请输入Key" />
        </n-form-item>
        <n-form-item label="Body" path="body">
          <n-input v-model:value="sendFormData.body" type="textarea" placeholder="请输入消息内容" />
        </n-form-item>
      </n-form>
      <template #footer>
        <n-space justify="end">
          <n-button @click="handleCancelSend">取消</n-button>
          <n-button type="primary" @click="handleConfirmSend" :loading="isSending" :disabled="isSending">发送</n-button>
        </n-space>
      </template>
    </n-modal>
  </n-space>
</template>

<script setup lang="ts">
import { ref, computed, h, onMounted } from 'vue'
import { Search, Refresh } from '@vicons/ionicons5'
import {
  fetchTopics as apiGetTopics,
  createTopic,
  updateTopic,
  deleteTopic,
  sendMessage,
  type TopicData,
} from '@/api/topicService'
import {
  NSpace,
  NButton,
  NInput,
  NIcon,
  NButtonGroup,
  NDataTable,
  NModal,
  NForm,
  NFormItem,
  NInputNumber,
  type DataTableColumns,
  type FormRules,
  type FormInst,
  useMessage,
  useDialog
} from 'naive-ui'
import { useRouter } from 'vue-router'

const message = useMessage()
const dialog = useDialog()
const formRef = ref<FormInst | null>(null)
const searchValue = ref('')
const tableData = ref<TopicData[]>([])
const loading = ref(false)
const showCreateDialog = ref(false)
const showEditDialog = ref(false)
const editingTopic = ref<string>('')
const showSendDialog = ref(false)
const sendFormRef = ref<FormInst | null>(null)
const sendingTopic = ref<string>('')
const router = useRouter()
const isSending = ref(false)

interface TopicFormData {
  topic: string
  partitionNum: number
  retentionDays: number
}

const formData = ref<TopicFormData>({
  topic: '',
  partitionNum: 8,
  retentionDays: 7
})

const rules: FormRules = {
  topic: [
    { required: true, message: '请输入Topic名称' },
    { pattern: /^[a-zA-Z0-9_-]+$/, message: 'Topic名称只能包含字母、数字、下划线和连字符' }
  ],
  partitionNum: [
    { required: true, message: '请输入分区数' },
    { type: 'number', min: 1, message: '分区数必须大于0' }
  ],
  retentionDays: [
    { required: true, message: '请输入消息保留时长' },
    { type: 'number', min: 1, message: '消息保留时长必须大于0' }
  ]
}

interface SendMessageFormData {
  tag: string
  key: string
  body: string
}

const sendFormData = ref<SendMessageFormData>({
  tag: '',
  key: '',
  body: ''
})

const sendRules: FormRules = {
  tag: [
    { required: true, message: '请输入Tag' }
  ],
  key: [
    { required: true, message: '请输入Key' }
  ],
  body: [
    { required: true, message: '请输入消息内容' }
  ]
}

const columns: DataTableColumns<TopicData> = [
  {
    type: 'selection',
    width: 50,
  },
  {
    title: 'Topic名称',
    key: 'topic',
    render(row) {
      return h(
        'a',
        {
          href: '#',
          style: {
            color: '#2d8cf0',
          },
          onClick: (e) => {
            e.preventDefault()
            router.push(`/topic/${row.topic}`)
          }
        },
        { default: () => row.topic }
      )
    }
  },
  { title: '分区数', key: 'partitionNum' },
  { title: '消息保留时长(天)', key: 'retentionDays' },
  { title: '消息总量', key: 'messageTotal' },
  {
    title: '操作',
    key: 'actions',
    render(row) {
      return h(
        NSpace,
        { size: 'small' },
        {
          default: () => [
            h(
              NButton,
              {
                text: true,
                type: 'primary',
                size: 'small',
                onClick: () => handleSendMessage(row)
              },
              { default: () => '发送消息' }
            ),
            h(
              NButton,
              {
                text: true,
                type: 'primary',
                size: 'small',
                onClick: () => handleEditTopic(row)
              },
              { default: () => '修改配置' }
            ),
            h(
              NButton,
              {
                text: true,
                type: 'error',
                size: 'small',
                onClick: () => handleDeleteTopic(row.topic)
              },
              { default: () => '删除' }
            )
          ]
        }
      )
    }
  }
]

const pagination = {
  pageSize: 10
}

const loadTopics = async () => {
  loading.value = true
  try {
    tableData.value = await apiGetTopics()
  } catch (error) {
    if (error instanceof Error) {
      message.error(error.message)
    } else {
      message.error('加载Topic列表失败')
    }
  } finally {
    loading.value = false
  }
}

const filteredData = computed(() => {
  if (!searchValue.value) return tableData.value
  return tableData.value.filter(item => item.topic.includes(searchValue.value))
})

const resetForm = () => {
  formData.value = {
    topic: '',
    partitionNum: 8,
    retentionDays: 7
  }
  editingTopic.value = ''
}

const handleCreateTopic = () => {
  resetForm()
  showCreateDialog.value = true
}

const handleConfirmCreate = async () => {
  if (!formRef.value) return

  try {
    await formRef.value.validate()
    await createTopic(formData.value)
    message.success('创建Topic成功')
    showCreateDialog.value = false
    loadTopics()
  } catch (error) {
    if (error instanceof Error) {
      message.error(error.message)
    } else {
      message.error('创建Topic失败')
    }
  }
}

const handleCancelCreate = () => {
  showCreateDialog.value = false
  resetForm()
}

const handleEditTopic = (topic: TopicData) => {
  editingTopic.value = topic.topic
  formData.value = {
    topic: topic.topic,
    partitionNum: topic.partitionNum,
    retentionDays: topic.retentionDays
  }
  showEditDialog.value = true
}

const handleConfirmEdit = async () => {
  if (!formRef.value) return

  try {
    await formRef.value.validate()
    await updateTopic(editingTopic.value, {
      partitionNum: formData.value.partitionNum,
      retentionDays: formData.value.retentionDays
    })
    message.success('修改Topic配置成功')
    showEditDialog.value = false
    loadTopics()
    resetForm()
  } catch (error) {
    if (error instanceof Error) {
      message.error(error.message)
    } else {
      message.error('修改Topic配置失败')
    }
  }
}

const handleCancelEdit = () => {
  showEditDialog.value = false
  resetForm()
}

const handleDeleteTopic = (topic: string) => {
  dialog.warning({
    title: '确认删除',
    content: `确定要删除Topic "${topic}" 吗？`,
    positiveText: '确定',
    negativeText: '取消',
    onPositiveClick: async () => {
      try {
        await deleteTopic(topic)
        message.success('删除Topic成功')
        loadTopics()
      } catch (error) {
        if (error instanceof Error) {
          message.error(error.message)
        } else {
          message.error('删除Topic失败')
        }
      }
    }
  })
}

const resetSendForm = () => {
  sendFormData.value = {
    tag: '',
    key: '',
    body: ''
  }
  sendingTopic.value = ''
}

const handleSendMessage = (topic: TopicData) => {

  resetSendForm()
  sendingTopic.value = topic.topic
  showSendDialog.value = true
}

const handleConfirmSend = async () => {
  if (!sendFormRef.value) return

  try {
    isSending.value = true
    await sendFormRef.value.validate()
    await sendMessage(sendingTopic.value, sendFormData.value)
    message.success('发送消息成功')
    showSendDialog.value = false
    resetSendForm()
  } catch (error) {
    if (error instanceof Error) {
      message.error(error.message)
    } else {
      message.error('发送消息失败')
    }
  } finally {
    isSending.value = false
  }
}

const handleCancelSend = () => {
  showSendDialog.value = false
  resetSendForm()
}

onMounted(() => {
  loadTopics()
})
</script>
