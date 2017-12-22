import Vue from 'vue'
import Router from 'vue-router'
import DataCenter from '@/components/DataCenter'
import Foo from '@/components/Foo'

Vue.use(Router)
export default new Router({
  routes: [
    {
      path: '/',
      name: 'DataCenter',
      component: DataCenter
    },
    {
      path: '/foo',
      name: 'Foo',
      component: Foo
    }
  ]
})
