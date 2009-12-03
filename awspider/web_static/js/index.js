
$(document).ready(function() { 
	$('#form_query').ajaxForm({"dataType":"json"})
	$('#form_check_peers').ajaxForm({"dataType":"json"})
	$('#form_pause').ajaxForm({"dataType":"json", "success":form_pause_callback})
	$('#form_resume').ajaxForm({"dataType":"json", "success":form_resume_callback})
	$('#form_shutdown').ajaxForm({"dataType":"json", "success":form_shutdown_callback, "beforeSubmit":form_shutdown_beforesubmit})
	$('#form_delete_reservation').ajaxForm({"dataType":"json", "success":form_delete_reservation_callback})
	$('#form_delete_function_reservations').ajaxForm({"dataType":"json", "success":form_delete_function_reservations_callback})
	$('#form_execute_reservation input[type=text]').clearingInput({"text":"UUID"})
	$('#form_delete_reservation input[type=text]').clearingInput({"text":"UUID"})
	$('#form_delete_function_reservations input[type=text]').clearingInput({"text":"Function name"});
	$('#form_show_reservation input[type=text]').clearingInput({"text":"UUID"});

	jQuery.ajax({
		"type": "GET",
		"url": "/data/server",
		"data": {},
		"dataType": "json",
		"success":data_server_callback
	})
	jQuery.ajax({
		"type": "GET",
		"url": "/data/exposed_function_details",
		"data": {},
		"dataType": "json",
		"success":data_exposed_function_details_callback
	})		

})



/*
*
* Data callbacks
*
*/

data_exposed_function_details_callback = function( data ) {

	for (i in data) {
		var exposed_function = data[i]
		
		if (exposed_function[1]["required_arguments"].length > 0) {
			var required_arguments = $.UL()
			for (j in exposed_function[1]["required_arguments"]) {
				$(required_arguments).append(
					$.LI({}, exposed_function[1]["required_arguments"][j])
				)
			}
		} else {
			var required_arguments = "None" 
		}
			
		if (exposed_function[1]["optional_arguments"].length > 0) {
			var optional_arguments = $.UL()
			for (j in exposed_function[1]["optional_arguments"]) {
				$(optional_arguments).append(
					$.LI({}, exposed_function[1]["optional_arguments"][j])
				)
			}
		} else {
			var optional_arguments = "None" 
		}
		
		$("#exposed_functions").append(
			$.H3({}, exposed_function[0])
		);

		$("#exposed_functions").append(
			$.TABLE({"class":"exposed_function"},
				$.TR({},
					$.TH({},"Interval"),
					$.TD({},exposed_function[1]["interval"] + " seconds")
				),
				$.TR({},
					$.TH({},"Required arguments"),
					$.TD({}, required_arguments)
				),
				$.TR({},
					$.TH({},"Optional arguments"),
					$.TD({}, optional_arguments)
				)
			)
		)
	}
}

data_server_callback = function( data ) {
	if( typeof data["running_time"] != "undefined" ) {
		var running_time = data["running_time"]
		var days=Math.floor(running_time / 86400)
		var hours = Math.floor((running_time- (days * 86400 ))/3600)
		var minutes = Math.floor((running_time - (days * 86400 ) - (hours *3600 ))/60)
		var secs = Math.floor((running_time - (days * 86400 ) - (hours *3600 ) - (minutes*60)))
		
		if (days == 1) {
			days = days + " day";
		} else {
			days = days + " days";
		}
		
		if (hours == 1) {
			hours = hours + " hour";
		} else {
			hours = hours + " hours";
		}
		
		if (minutes == 1) {
			minutes = minutes + " minute";
		} else {
			minutes = minutes + " minutes";
		}
		
		if (secs == 1) {
			secs = secs + " second";
		} else {
			secs = secs + " seconds";
		}
		var message = "The web server has been running for " + days + ", " + hours + ", " + minutes + ", and " + secs + ".";
		if( typeof data["cost"] != "undefined" ) {
			message = message + " At its current rate, the spider will cost about $" + Math.round(data["cost"]*Math.pow(10,2))/Math.pow(10,2) + " per month.";
		}
		$("#running_time").html(message).fadeIn()
	}
	if(typeof data["current_timestamp"] != "undefined") {
		$("#current_timestamp").html(data["current_timestamp"]).fadeIn()
	}
	if(typeof data["paused"] != "undefined") {
		if( data["paused"] ) {
			pause()
		} else {
			resume()
		}
	}
	if(typeof data["pending_requests_by_host"] != "undefined") {
		var message = [];
		for(var host in data["pending_requests_by_host"]) {
			if(parseInt(data["pending_requests_by_host"][host]) > 0) {
				message.push("<b>" + host + ":</b> " + data["pending_requests_by_host"][host])
			}
		}
		$("#pending_requests_by_host").html(message.join(", ")).fadeIn()
	}
	if(typeof data["active_requests_by_host"] != "undefined") {
		var message = [];
		for(var host in data["active_requests_by_host"]) {
			if(parseInt(data["active_requests_by_host"][host]) > 0) {
				message.push("<b>" + host + ":</b> " + data["active_requests_by_host"][host])
			}
		}
		$("#active_requests_by_host").html(message.join(", ")).fadeIn()
	}
	if(typeof data["active_requests"] != "undefined") {
		$("#active_requests").html(data["active_requests"]).fadeIn()
	}
	if(typeof data["pending_requests"] != "undefined") {
		$("#pending_requests").html(data["pending_requests"]).fadeIn()
	}
}

/*
*
* Form callbacks
*
*/

form_delete_reservation_callback = function( data ) {
	$(this).find("input[type=text]").each( function(){
		this.value = "";
		this.blur();
	})
}

form_delete_function_reservations_callback = function( data ) {
	$(this).find("input[type=text]").each( function(){
		this.value = "";
		this.blur();
	})	
}

form_shutdown_callback = function( data ){
	window.location="docs/shutdown"
}

form_shutdown_beforesubmit = function( data ) {
	return confirm("The spider cannot be restarted from the web interface. Continue?")
}
form_pause_callback = function( data ) {
	pause()
}

form_resume_callback = function( data ) {
	resume()
}

/*
*
* Basic DOM functions 
*
*/

pause = function() {
	$("#paused").fadeIn()
	$('#form_pause input').each(function() {this.disabled=true})
	$('#form_resume input').each(function() {this.disabled=false})
}

resume = function() {
	$("#paused").fadeOut()
	$('#form_pause input').each(function() {this.disabled=false})
	$('#form_resume input').each(function() {this.disabled=true})
}
