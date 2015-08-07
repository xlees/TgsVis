

// 定义一个控件类,即function
var myControl = function () {
  // 默认停靠位置和偏移量
  this.defaultAnchor = BMAP_ANCHOR_TOP_RIGHT;
  this.defaultOffset = new BMap.Size(10, 10);
}

// 通过JavaScript的prototype属性继承于BMap.Control
myControl.prototype = new BMap.Control();

// 自定义控件必须实现自己的initialize方法,并且将控件的DOM元素返回
// 在本方法中创建个div元素作为控件的容器,并将其添加到地图容器中
myControl.prototype.initialize = function(map) {

    // div.text('degree');
    var div = $("<div id='div-degree'></div>");

    $.getJSON('/load-map-control',{},function (json) {
        div.html(json.data);
    });

    // var div = document.createElement("div");
    // $('#div-degree').load("{{ url_for('static', filename='in_out.html') }}", {}, function() {
    //     // $(this).fadeIn('slow');
    // });

    // var div = $("<div id='div-degree'></div>");
    // div.html('<b>创建</b>');

    // var indegree = $('<label></label>');
    // indegree.addClass('btn btn-default');
    // indegree.append("<input type='radio' name='options' id='indegree' value='tgs-all'>全部");
    // indegree.appendTo(div);

  // // 创建一个DOM元素
  // var div = document.createElement("div");
  // // 添加文字说明
  // div.appendChild(document.createTextNode("放大2级"));
  // // 设置样式
  // div.style.cursor = "pointer";
  // div.style.border = "1px solid gray";
  // div.style.backgroundColor = "white";
  // // 绑定事件,点击一次放大两级
  // div.onclick = function(e){
  //   map.setZoom(map.getZoom() + 2);
  // }

  // 添加DOM元素到地图中
  map.getContainer().appendChild(div[0]);

  return div[0];
}


// 创建控件
ctrl = new myControl();

// 添加到地图当中
// map.addControl(ctrl);

