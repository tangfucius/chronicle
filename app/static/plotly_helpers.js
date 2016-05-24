//options is a list of strings
//for selectize plugin
function selectize(selector, options) {
	var $select = $(selector).selectize({
				plugins: ['remove_button'],
				maxItems: null,
				valueField: 'id',
				labelField: 'id',
				searchField: 'id',
				options: options.map(function(v){
					return {id: v}
				}),
				/*create: function(input) {
					return {id: input}
				}*/
			});
	return $select;
}

function addDays(date, days) {
    var d = new Date(date);
    d.setDate(d.getDate() + days);
    month = '' + (d.getMonth() + 1),
    day = '' + d.getDate(),
    year = d.getFullYear();

    if (month.length < 2) month = '0' + month;
    if (day.length < 2) day = '0' + day;

    return [year, month, day].join('-');
}

function verticalLine(x) {
	var obj = {
      type: 'line',
      xref: 'x',
  	  yref: 'paper',
      x0: x,
      y0: 0,
      x1: x,
      y1: 1,
      line: {
        color: 'rgb(128, 0, 128)',
        width: 1,
        dash: 'dash'
      }
    }
    return obj;
}

function shape(sd, duration) {
        var obj = {
        	type: 'rect',
        // x-reference is assigned to the x-values
            xref: 'x',
            // y-reference is assigned to the plot paper [0,1]
            yref: 'paper',
            x0: sd,
            y0: 0,
            x1: addDays(sd, duration),
            y1: 1,
            fillcolor: '#d3d3d3',
            opacity: 0.2,
            line: {
                width: 0
            }
    	}
    	return obj;
}

function plotly_chart(chart_data, el) {
	console.log(chart_data);
	var xys = chart_data['xys']

	var y2Flag = false
	for (var l=0; l<xys.length; l++) {
		if ('yaxis' in xys[l] && xys[l]['yaxis']=='y2') {
			y2Flag = true 
		}
	}

	var annos = chart_data['annos']
	var annos_scatter = {'name': 'annotations', x:[], y:[], mode: 'markers', type: 'scatter'}
	//var gridPos = chart_data['grid']
	//console.log(gridPos);
	var shapes = []
	var annoAttr = {
			xref: 'x',
	      	//yref: 'paper',
	      	showarrow: true,
	      	arrowhead: 3,
	      	ax: 20,
	      	ay: -40
	      }

	for (var i=0; i<annos.length; i++) {
		var a = annos[i];
		annos_scatter.x.push(a.x);
		annos_scatter.y.push(a.y);
		for (var attrname in annoAttr) {
			a[attrname] = annoAttr[attrname];
		}
		if ('link' in a) {
			if (!a['link'].startsWith('http://')) a['link'] = 'http://'+a['link'];
			a['text'] += '<br><a href="'+a['link']+'">Link</a>';
		}


		console.log(a);
		var newVert = verticalLine(a['x']);
		shapes.push(newVert);
			
		if (a.duration>1) {
			var newShape = shape(a['x'], a.duration);
			shapes.push(newShape);
		}
	}

	xys.push(annos_scatter);
	console.log(xys);

	var layout = {
		margin: {
		    l: 40,
		    r: 30,
		    b: 30,
		    t: 5,
		    pad: 0
		  },
		  legend: {
		    //showLegend: true,
		    //y: 1,
		    traceorder: 'normal',
		    font: {
		      family: 'sans-serif',
		      size: 10,
		      color: '#000'
		   }},
		  //annotations: annos,
		  shapes: shapes
		};

	if (y2Flag) {
		layout['yaxis2'] = {
		    overlaying: 'y',
		    side: 'right'
		  }
	}

	Plotly.newPlot(el, xys, layout);

	for (var i=0; i<annos.length; i++) {
		var a = annos[i];
		for (var attrname in annoAttr) {
			a[attrname] = annoAttr[attrname];
		}
		Plotly.relayout(el, 'annotations[' + i + ']', a);
	}
}