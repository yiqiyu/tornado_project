var data_dict = {
    'submit_dist': {req: "/api/search", args: {jobarea: "txt_1"}},
    "submit_tags": {req: "/api/get_tags", args: {jobarea: "txt_21", key: "txt_22"}},
};

function fillData(obj) {
    $("#"+obj.id).attr("data", obj.value);
}

function getCityCode(obj) {
    if (obj.value.length==0){
//      $("#" +tag).attr("disabled", "disabled");
      return;
      }
    $.post("/", {name: obj.value},
                function(data) {
                    if (data != null) {
                        $("#" +obj.id).attr("data", data);
                    }
                  });

}

function make_data(anayl) {
    var tmp = {};
    $.extend(tmp, data_dict[anayl]["args"]);
    for (var arg in data_dict[anayl]["args"]) {
        tmp[arg] = $("#" +data_dict[anayl]["args"][arg]).attr("data")
    }
    return $.extend({ajax: true}, tmp)
}

function validate(anayl) {
    var res = 1;
    for (var arg in data_dict[anayl]["args"]) {
        res = res && $("#" +data_dict[anayl]["args"][arg]).attr("data")
    }
    return res
}

function launchSpider(obj) {
    //var req = "/api/search?ajax=true&jobarea=" + $("#submit").attr("data");
    //location.href=req;
    var anayl = obj.id
    if (!validate(anayl)) {
        alert("请将必选项目填写完整！")
        return;
    }
    var req = data_dict[anayl]["req"];
    $.ajax({
    url: req,
    anayl: "GET",
    data: make_data(anayl),
    success: renderGraph,
    beforeSend: waitingResults,
    });
    return;
}

function renderGraph(raw_data) {
    var option;
    var data = JSON.parse(raw_data)
    if (data["type"] == "dist") {
        option = {
            title: {
                text: '行业招聘分布'
            },
            tooltip: {
                show: true
            },
            series: [{
            radius: '55%',
            name: '数量',
            type: 'pie',
            data: data["data"]
            }]
        };
    } else if (data["type"] == "tags") {
        option = {
            title: {
                text: '职位关键字'
            },
            tooltip: {
            show: true
            },

            series: [{
            type: 'wordCloud',
            size: ['80%', '80%'],
            textRotation : [0, 30],
            textPadding: 0,
            autoSize: {
                enable: true,
                minSize: 14
            },
            data: data["data"]
            }]
            };
    }


    $("#loading").remove()
    var myChart = echarts.init(document.getElementById("display"));
    myChart.setOption(option);

}

