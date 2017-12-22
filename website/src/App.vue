<template>
  <div id="app">
    <router-view></router-view>
    <div class="top-tile-bar">
      <b-navbar toggleable="md" type="dark" variant="info">
        <b-navbar-toggle target="nav_collapse"></b-navbar-toggle>
        <b-navbar-brand href="#">Venus数据系统</b-navbar-brand>
        <b-collapse is-nav id="nav_collapse">
          <!-- Right aligned nav items -->
          <b-navbar-nav class="ml-auto">

            <b-nav-form>
              <b-form-input size="sm" class="mr-sm-2" type="text" placeholder="Search"/>
              <b-button size="sm" class="my-2 my-sm-0" type="submit">Search</b-button>
            </b-nav-form>
            <b-nav-item-dropdown right>
              <!-- Using button-content slot -->
              <template slot="button-content">
                <em>当前用户</em>
              </template>
              <b-dropdown-item href="#">Profile</b-dropdown-item>
              <b-dropdown-item href="#">Signout</b-dropdown-item>
            </b-nav-item-dropdown>
          </b-navbar-nav>
        </b-collapse>
      </b-navbar>

    </div>
    <div class="container-fluid">
      <div class="row flex-x1-nowrap2">
        <div class="bd-sidebar col-md-3 col-xl-1 col-12">
          <b-nav class="bd-links" vertical>
            <b-nav-item style="text-align: left"><router-link to="/foo">数据监控中心</router-link></b-nav-item>
            <b-nav-item style="text-align: left">万得数据提取</b-nav-item>
            <b-nav-item style="text-align: left">本地数据库提取</b-nav-item>
          </b-nav>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
export default {
  name: 'app'
}
</script>

<style>
#app {
  font-family: 'Avenir', Helvetica, Arial, sans-serif;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  text-align: center;
  color: #2c3e50;
  margin-top: 10px;
}

@import 'css/fontawesome-free-5.0.2/web-fonts-with-css/css/fontawesome-all.min.css'
.top-tile-bar {
  margin-right:2px;
  margin-left:2px;
}
.row {
  display: flex;
  flex-wrap: wrap;
  margin-right: -15px;
  margin-left: -15px;
}
.bd-sidebar {
  order: 0;
  background-color: #f5f2f9;
  border-bottom: 1px solid rgba(0, 0, 0, .1);
@include media-breakpoint-up(md) {
  @supports (position: sticky) {
    position: sticky;
    top: 4rem;
    z-index: 1000;
    height: calc(100vh - 4rem);
  }
  border-right: 1px solid rgba(0, 0, 0, .1);
}
@include media-breakpoint-up(xl) {
  max-width: 320px;
}
}

.bd-links {
  padding-top: 1rem;
  padding-bottom: 1rem;
  margin-right: -15px;
  margin-left: -15px;

@include media-breakpoint-up(md) {
  @supports (position: sticky) {
    max-height: calc(100vh - 9rem);
    overflow-y: auto;
  }
}

// Override collapse behaviors
@include media-breakpoint-up(md) {
  display: block !important;
}
}
</style>
