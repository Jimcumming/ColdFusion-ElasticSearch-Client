component accessors="true"{

	property name="active" type="struct";
	property name="serverList" type="struct";
	property name="inactive" type="struct";
	property name="ClusterName" type="string";

	public ClusterManager function init(any nodeConfig=""){
		variables.active = {};
		variables.inactive = {};
		loadConfigFromString(arguments.nodeConfig);
		return this;
	}

	private ClusterManager function loadConfigFromString(required any nodeConfig){
		/*
			config = [{
				host = "",
				port = "",
				path = "",
				secure = "",
				username = "",
				password = ""
			}]
		*/
		var config = "";
		if(isSimpleValue(arguments.nodeConfig) && len(trim(arguments.nodeConfig))){
			if(isJson(arguments.nodeConfig)){
				config = deserializeJSON(arguments.NodeConfig);
			}else{
				throw(message="The node config passed to the ElasticSearch ClusterManager is not a valid JSON string.");
			}
		}else if(isArray(arguments.nodeConfig)){
			config = arguments.nodeConfig;
		} else {
			return this;
		}

		for(var c=1; c<=arrayLen(config); c++){
			addNode(new NodeConfig(argumentCollection=config[c]));
		}
		return this;
	}

	public ClusterManager function addNode(required NodeConfig NodeConfig){
		variables.active[arguments.NodeConfig.getServerId()] = arguments.NodeConfig;
		updateServerList();
		return this;
	}

	public ClusterManager function updateServerList(){
		variables.serverList = {active = structKeyList(getActive()), inactive = structKeyList(getInactive())};
		return this;
	}
	
	public string function getEndpoint(){
		 return variables.active[listGetAt(variables.serverList.active, RandRange(1,listLen(variables.serverList.active)))].url();
	}

	public struct function doRequest(string Endpoint=getEndPoint(), required string Resource, string Method="GET", string Body="", string ResponseType="Response"){

		var sendResult = {};


		if(len(trim(Arguments.Body)) && find("@", arguments.endpoint)){
			// body content and basic auth
			cfhttp(method=arguments.method, charset="utf-8", timeout=30, url=arguments.endpoint  & Arguments.Resource, result="sendResult"
					authType="basic", username=listFirst(basicAuth, ":"), password=listLast(basicAuth, ":")) {
		    	cfhttpparam(type="body", value=arguments.body);
			}
		} else if(len(trim(Arguments.Body))){
			// body content
			cfhttp(method=arguments.method, charset="utf-8", timeout=30, url=arguments.endpoint  & Arguments.Resource, result="sendResult") {
		    	cfhttpparam(type="body", value=arguments.body);
			}
		} else if (find("@", arguments.endpoint)) {
			// basic auth
			cfhttp(method=arguments.method, charset="utf-8", timeout=30, url=arguments.endpoint  & Arguments.Resource, result="sendResult"
					authType="basic", username=listFirst(basicAuth, ":"), password=listLast(basicAuth, ":")) {
			}
		} else {
			// none of the above
			cfhttp(method=arguments.method, charset="utf-8", timeout=30, url=arguments.endpoint  & Arguments.Resource, result="sendResult") {}
		}

		var response = createObject("component", "responses.#arguments.ResponseType#").init();

		response.handleResponse(sendResult);
		return response;

	}

}