$(function() {
    var rootPath = "http://127.0.0.1:9003"
    $("#user-content").height(400);
    var user_chart = echarts.init(document.getElementById('user-content'));
    $.ajax({
        url: "/statsUser/selectAll",
        method: "post",
        success: function(res) {
            if(res.code === 1001) {
                stats_user_chart(res.data);
            }
        }
    })
    
    /** 用户基本信息 图 */
    function stats_user_chart(data) {
        var  option = {
            title: {
                text: ''
            },
            tooltip: {
                trigger: 'axis'
            },
            legend: {
                data:['活跃用户','新增用户','总用户','活跃会员','新增会员','总会员','Session数量','总Session时长']
            },
            grid: {
                left: '3%',
                right: '4%',
                bottom: '3%',
                containLabel: true
            },
            toolbox: {
                feature: {
                    saveAsImage: {}
                }
            },
            xAxis: {
                type: 'category',
                boundaryGap: false,
                data: ['2018-12-05','2018-12-06','2018-12-07','2018-12-08','2018-12-09','2018-12-10','2018-12-11']
            },
            yAxis: {
                type: 'value'
            },
            series: [
                {
                    name:'活跃用户',
                    type:'line',
                    stack: '总量',
                    data:[120, 132, 101, 134, 90, 230, 210]
                },
                {
                    name:'新增用户',
                    type:'line',
                    stack: '总量',
                    data:[220, 182, 191, 234, 290, 330, 310]
                },
                {
                    name:'总用户',
                    type:'line',
                    stack: '总量',
                    data:[150, 232, 201, 154, 190, 330, 410]
                },
                {
                    name:'活跃会员',
                    type:'line',
                    stack: '总量',
                    data:[320, 332, 301, 334, 390, 330, 320]
                },
                {
                    name:'新增会员',
                    type:'line',
                    stack: '总量',
                    data:[400, 932, 901, 934, 1290, 1330, 1320]
                },
                {
                    name:'总会员',
                    type:'line',
                    stack: '总量',
                    data:[500, 932, 901, 934, 1290, 1330, 1320]
                },
                {
                    name:'Session数量',
                    type:'line',
                    stack: '总量',
                    data:[600, 932, 901, 934, 1290, 1330, 1320]
                },
                {
                    name:'总Session时长',
                    type:'line',
                    stack: '总量',
                    data:[700, 932, 901, 934, 1290, 1330, 1320]
                }
            ]
        };
        user_chart.setOption(option);
    }

    












































});