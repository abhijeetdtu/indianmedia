Vue.component('dash-app', {
  props: ['url' , "height" , "width"],
  data : function(){
    return {
        content : ""
    };
  },
  template: `
    <iframe v-bind:src="url" v-bind:width="width" v-bind:height="height"></iframe>
  `,
  methods:{
    load: function(){
      self = this;
      this.$http.get(this.url).then(function(response){
        data = response.body;
        self.content =data;
      });
    }
  },
  mounted: function(){this.load();}

});
