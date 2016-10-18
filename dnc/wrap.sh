#!/bin/bash
disable_proxy()
{
    networksetup -setsocksfirewallproxystate Wi-Fi off
    networksetup -setsocksfirewallproxystate Ethernet off
    echo "SOCKS proxy disabled."
}
trap disable_proxy INT
 
networksetup -setsocksfirewallproxy Wi-Fi 127.0.0.1 9999
networksetup -setsocksfirewallproxy Ethernet 127.0.0.1 9999
networksetup -setsocksfirewallproxystate Wi-Fi on
networksetup -setsocksfirewallproxystate Ethernet on
echo "SOCKS proxy enabled."
echo "Tunneling..."


'-setwebproxystate', 
'-setsecurewebproxystate'

'-setwebproxy', 
'-setsecurewebproxy'