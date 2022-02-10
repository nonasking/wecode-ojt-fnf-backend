def check_keys_in_dictionary(check_dict:dict, necessary_key_list:list):

    check_dict_keys = check_dict.keys()
    
    if not set(necessary_key_list).issubset(set(check_dict_keys)):
        required_keys = set(necessary_key_list) \
                      - set(necessary_key_list).intersection(set(check_dict_keys))

        error_message='' 
        for required_key in required_keys:
            error_message += required_key + ", "

        raise KeyError(f"[{error_message}] was not found in request")
