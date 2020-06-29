Vue.component('dash-app', {
  props: ['url' , "classname"],
  template: `
    <div class="shadow-sm mb-2 bg-white rounded row dash-app">
      <div class="col-md col-md-almost-full">
        <div class="row">
          <div class="col-md-12">
            <div class="embed-responsive" v-bind:class="classname">
              <!-- <iframe v-bind:src="url"  class="embed-responsive-item"></iframe> -->
              <vue-friendly-iframe v-bind:src="url"></vue-friendly-iframe>
            </div>
          </div>
        </div>
      </div>
      <div class="col-md-almost-nill">
        <button type="button" class="btn btn-outline-danger btn-sm" v-on:click="deleteApp()">X</button>
      </div>
    </div>
  `,
  methods:{
    deleteApp : function(){
      this.$emit('delete', this.url)
    }
  }
});
