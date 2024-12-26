import { createRouter, createWebHistory } from 'vue-router'
import Console from '../views/Console.vue'
import TopicDetail from '../views/TopicDetail.vue'

const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes: [
    {
      path: '/',
      name: 'topic',
      component: Console,
    },
    {
      path: '/topic/:topic',
      name: 'topicDetail',
      component: TopicDetail,
    },
  ],
})

export default router
