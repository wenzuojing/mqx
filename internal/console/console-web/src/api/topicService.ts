import axios from 'axios'

const BASE_URL = ''

export interface TopicData {
  topic: string
  partitionNum: number
  retentionDays: number
  messageTotal: number
}

export interface CreateTopicParams {
  topic: string
  partitionNum: number
  retentionDays: number
}

export interface UpdateTopicParams {
  partitionNum: number
  retentionDays: number
}

export interface SendMessageParams {
  tag: string
  key: string
  body: string
}

export interface ConsumerGroup {
  group: string
  clientCount: number
  delay: number
}

export interface Partition {
  partition: number
  stat: {
    maxOffset: number
    minOffset: number
    total: number
  }
}

export interface ConsumerOffset {
  partition: number
  offset: number
  instanceId: string
  hostname: string
  active: boolean
  maxOffset: number
  minOffset: number
}

export interface Message {
  messageId: string
  tag: string
  key: string
  body: string
  bornTime: string
}

export interface QueryMessageParams {
  pageNo: number
  pageSize: number
  topic: string
  partition: number
  messageId?: string
  tag?: string
}

export const fetchTopics = async (): Promise<TopicData[]> => {
  const response = await axios.get(`${BASE_URL}/api/topics`)
  if (response.data.error) {
    throw new Error(response.data.error)
  }
  return response.data.topics
}

export const createTopic = async (params: CreateTopicParams): Promise<void> => {
  const response = await axios.post(`${BASE_URL}/api/topics`, params)
  if (response.data.error) {
    throw new Error(response.data.error)
  }
}

export const updateTopic = async (topic: string, params: UpdateTopicParams): Promise<void> => {
  const response = await axios.put(`${BASE_URL}/api/topics/${topic}`, params)
  if (response.data.error) {
    throw new Error(response.data.error)
  }
}

export const deleteTopic = async (topic: string): Promise<void> => {
  const response = await axios.delete(`${BASE_URL}/api/topics/${topic}`)
  if (response.data.error) {
    throw new Error(response.data.error)
  }
}

export const sendMessage = async (topic: string, params: SendMessageParams): Promise<void> => {
  const response = await axios.post(`${BASE_URL}/api/topics/${topic}/messages`, params)
  if (response.data.error) {
    throw new Error(response.data.error)
  }
}

export const getConsumerGroups = async (topic: string): Promise<ConsumerGroup[]> => {
  const response = await axios.get(`${BASE_URL}/api/topics/${topic}/consumer-groups`)
  if (response.data.error) {
    throw new Error(response.data.error)
  }
  return response.data.consumerGroups
}

export const getPartitions = async (topic: string): Promise<Partition[]> => {
  const response = await axios.get(`${BASE_URL}/api/topics/${topic}/partitions`)
  if (response.data.error) {
    throw new Error(response.data.error)
  }
  return response.data.partitions
}

export const getConsumerGroupOffsets = async (
  topic: string,
  group: string,
): Promise<ConsumerOffset[]> => {
  const response = await axios.get(
    `${BASE_URL}/api/topics/${topic}/consumer-groups/${group}/offsets`,
  )
  if (response.data.error) {
    throw new Error(response.data.error)
  }
  return response.data.offsets
}

export const queryMessages = async (params: QueryMessageParams): Promise<Message[]> => {
  const response = await axios.get(`${BASE_URL}/api/messages`, { params })
  if (response.data.error) {
    throw new Error(response.data.error)
  }
  return response.data.messages
}
