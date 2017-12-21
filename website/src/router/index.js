import Vue from 'vue'
import Router from 'vue-router'
import Hello from '@/components/Hello'
import DataCenter from '@/components/DataCenter'

Vue.use(Router)

export default new Router({
  routes: [
    {
      path: '/',
      name: 'Hello',
      component: Hello
    },
    {
      path: '/datacenter',
      name: 'DataCenter',
      component: DataCenter
    }
  ]
})
