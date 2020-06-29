var app = new Vue({
  el: '#vue-app',
  delimiters: ['[[',']]'],
  data: {
    message: 'Hello Vue!',
    dashApps:[]
  },
  methods:{
    loadHelp: function(url){
      console.log(url)
      url = "/help/"+url
      this.loadDashApp(url)
    },
    loadDashApp : function(url){
      // For correct ordering of apps while displaying
      var r = Math.floor(Math.random()*1000000)
      url = url + r
      //Add to DashApps array
      this.dashApps.unshift(url)
        // self = this;
        // staging_selector = "#dash-app-staging"
        // staging_div = $(staging_selector)
        // staging_div.load(url , function(){
        //    window.setTimeout(function(){
        //      html = staging_div.html()
        //      self.dashApps.push(html)
        //      staging_div.html("")
        //    },1000)
        // });
    },
    deleteDashApp:function(url){
      this.dashApps = this.dashApps.filter(function(d){
        return d != url;
      })
    }
  }
})
