# from converter.parse_file_converter import ParseFileConverter
# from converter.simulink_converter import SimulinkConverter
#
# if __name__ == '__main__':
#     converter = SimulinkConverter()
#     converter.convert()


if __name__ == '__main__':
    url = 'http://10.10.3.13:18082/group1/originalData/zgq_simulation_test/????????/3885/Hozon_EP12_0361_Tianjin_20240119150445049000.vsb'
    url = url.replace('http://10.10.3.13:18082/group1/originalData/','')
    print('/'.join(url.split('/')[:-1]))
    print(url)
